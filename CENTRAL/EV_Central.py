import sys, socket, threading, json, time, curses, os
from database import Database
from chargingpoint import ChargingPoint
from queue import Queue
from kafka import KafkaConsumer, KafkaProducer
from cryptography.fernet import Fernet
from datetime import datetime

HEADER=10
FORMAT='utf-8'
COLA=Queue()
creado=not os.path.exists("central.log")
LOG=open("central.log", "a", buffering=1, encoding=FORMAT)
if creado:
    COLA.put("Central.log creado.")
try:
    TIMEOUT=int(sys.argv[6])
except (IndexError, ValueError):
    TIMEOUT=10
print(f"Central: TIMEOUT configurado a {TIMEOUT}s.")
TIMEOUT=10
PARADAS_PENDIENTES={}
REANUDOS_PENDIENTES={}
READY_PENDIENTES={}
DRIVER_PENDIENTES={}
CARGAS_PENDIENTES={}
ALERTAS_CLIMA={}
CP_PARADOS_CLIMA={}
PARADAS_CLIMA_PENDIENTES={}
lock=threading.Lock()

print("CENTRAL arrancada.")
# Cargamos la direccion de la BD.
DB_HOST=sys.argv[4]
DB_PUERTO=int(sys.argv[5])
db=Database(DB_HOST, DB_PUERTO, "root", "1234", "evcharging")
# Carga los CPs de la BD.
print("CENTRAL: Cargando los CPs de la BD...")
cps=db.getAllCPs()
# Pone todos los CPs en desconectado.
for i in cps.values():
    i.estado="DESCONECTADO"
    i.clave_simetrica=None
# Mostrar CPs cargados de la BD.
if(len(cps)!=0):
    print(f"CENTRAL: Proceso terminado.\nCENTRAL: Se cargaron {len(cps)} CPs.")
else:
    print(f"CENTRAL: Proceso terminado.\nCENTRAL: Se cargaron 0 CPs.")
# Carga los Drivers de la BD.
print("CENTRAL: Cargando los Drivers de la BD...")
drivers=db.getAllDrivers()
if(len(drivers)!=0):
    print(f"CENTRAL: Proceso terminado.\nCENTRAL: Se cargaron {len(drivers)} Drivers.")
else:
    print(f"CENTRAL: Proceso terminado.\nCENTRAL: Se cargaron 0 Drivers.")
# Cargamos la direccion Kafka.
KAFKA_HOST=sys.argv[2]
KAFKA_PUERTO=int(sys.argv[3])

producer=KafkaProducer(
    bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PUERTO}",
    value_serializer=lambda v: json.dumps(v).encode(FORMAT)
)

consumer=KafkaConsumer(
    bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PUERTO}",
    value_deserializer=lambda v: json.loads(v.decode(FORMAT)),
    group_id="central_group",
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Nos suscribimos a las dos colas de Kafka.
consumer.subscribe(topics=["cp_status", "driver_requests"])
print(f"CENTRAL: Conectado con Kafka en {KAFKA_HOST}:{KAFKA_PUERTO}")

def enviar_kafka_cifrado(cp_id, data_dict):
    with lock:
        cp = cps.get(cp_id)
    
    if cp and cp.clave_simetrica:
        try:
            json_str = json.dumps(data_dict)
            token_bytes = cp.clave_simetrica.encrypt(json_str.encode('utf-8'))

            mensaje = {
                "cp_id": cp_id,
                "payload_cifrado": token_bytes.decode('utf-8')
            }
            producer.send("cp_commands", mensaje)
            producer.flush()
        except Exception as e:
            COLA.put(f"CENTRAL: Error cifrando orden para CP[{cp_id}]: {e}")
    else:
        # Fallback si no hay clave (o mensaje para CP desconectado/sin seguridad)
        producer.send("cp_commands", data_dict)
        producer.flush()

# Aplicamos las paletas para cada estado.
def color_for(estado):
    if estado in ("ACTIVADO", "SUMINISTRANDO"):
        return curses.color_pair(1)
    if estado=="PARADO":
        return curses.color_pair(2)
    if estado=="AVERIADO":
        return curses.color_pair(3)
    if estado=="DESCONECTADO":
        return curses.color_pair(5)
    return curses.A_NORMAL

def run(stdscr):
    # Ocultar raton.
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.keypad(True)
    # Activar colores.
    curses.start_color()
    curses.use_default_colors()
    # Definicion de paletas.
    curses.init_pair(1, curses.COLOR_GREEN, -1) # Activado.
    curses.init_pair(2, curses.COLOR_YELLOW, -1) # Parado.
    curses.init_pair(3, curses.COLOR_RED, -1) # Averiado.
    curses.init_pair(4, curses.COLOR_CYAN, -1) # Cabecera.
    curses.init_pair(5, curses.COLOR_WHITE, -1) # Deconectado.

    row=0
    while True:
        # Limpiar pantalla.
        stdscr.erase()
        # Comandos.
        stdscr.addstr(0, 0, "Central · Comandos (↑/↓ mover · p Parar · r Reanudar · P Parar todos · R Reanudar todos · x Revocar clave · q Salir)", curses.color_pair(4) | curses.A_BOLD)

        with lock:
            items=sorted(cps.items(), key=lambda x: x[0])
        n=len(items)
        if row>=n:
            row=max(0, n-1)
        # Cabecera.
        stdscr.addstr(2, 0, "  ID      Ubicación                           €/kWh   Estado             Info")
        stdscr.addstr(3, 0, "-"*90)
        # Filas.
        line=4
        for idx, (cid, cp) in enumerate(items):
            estado=cp.estado or "DESCONECTADO"
            attr=color_for(estado)
            if idx==row:
                sel=">"
            else:
                sel=" "
            info=""
            secure_flag = "[SEC]" if getattr(cp, 'clave_simetrica', None) else "[UNSEC]" #????
            if estado=="SUMINISTRANDO":
                with lock:
                    carga=CARGAS_PENDIENTES.get(cid)
                    did=DRIVER_PENDIENTES.get(cid)
                if carga:
                    try:
                        kwh=float(carga.get("cp_carga"))
                    except Exception:
                        kwh=0.0
                    try:
                        euro=round(kwh * float(cp.precio), 2)
                    except Exception:
                        euro=0.0
                    info= f"{kwh:.2f} kWh · {euro:.2f} € · driver {did}"
            stdscr.addstr(line, 0,
                f"{sel} {cid:<4}    {str(cp.localizacion)[:30]:<30}    {float(cp.precio):>6.2f}    {estado:<15}  {secure_flag}  {info}",
                attr | (curses.A_REVERSE if idx==row else 0)
            )
            line+=1
        # Hora del sistema.
        stdscr.addstr(line+1, 0, time.strftime("Hora %H:%M:%S"))
        stdscr.refresh()

        # input no bloqueante
        try:
            k=stdscr.getch()
        except:
            k=-1

        if k==curses.KEY_UP and n:
            row=(row-1)%n
        elif k==curses.KEY_DOWN and n:
            row=(row+1)%n
        # Comando Salir.
        elif k in (ord('q'), ord('Q')):
            break
        # Comandos de Parar.
        elif k in (ord('p'), ord('P')):
            # Todos.
            if k==ord('P'):
                threading.Thread(target=do_parar, daemon=True).start()
            # Uno.
            elif n:
                threading.Thread(target=do_parar, args=([items[row][0]],), daemon=True).start()
        # Comandos de Reanudar.
        elif k in (ord('r'), ord('R')):
            # Todos.
            if k==ord('R'):
                threading.Thread(target=do_reanudar, daemon=True).start()
            # Uno.
            elif n:
                threading.Thread(target=do_reanudar, args=([items[row][0]],), daemon=True).start()
        elif k in (ord('x'), ord('X')):
            if n:
                 threading.Thread(target=do_revocar, args=([items[row][0]],), daemon=True).start()
        time.sleep(0.1)

def interfaz():
    curses.wrapper(run)

def enviar(conn,addr, msg):
    contenido=msg.encode(FORMAT)
    cabecera=(str(len(contenido))).encode(FORMAT)
    cabecera+=b' '*(HEADER-len(cabecera))
    conn.sendall(cabecera+contenido)
    COLA.put(f"CENTRAL: Mensaje \"{msg}\" enviado a {addr}.")

def recv_all(conn, n):
    data=b''
    try:
        while len(data)<n:
            packet=conn.recv(n-len(data))
            if not packet:
                return None
            data+=packet
        return data
    except (socket.timeout, OSError): 
        return None

# Hilo que imprime en Central.log
def qprint():
    while True:
        msg=COLA.get()
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"[{timestamp}] {msg}"
            LOG.write(log_entry+"\n")
        finally:
            COLA.task_done()

# Hilo que gestiona los mensajes de registro del CP.
def rec_cpn(conn, addr, resto):
    try:
        param=resto.split("|")
        cp_id=int(param[0])
        cp_loc=param[1]
        cp_prec=float(param[2])
        cp_token=param[3]
    except (ValueError, IndexError) as e:
        COLA.put(f"CENTRAL: CPN con mal formado")
        return
    key = Fernet.generate_key()       
    key_str = key.decode('utf-8')    
    cifrador = Fernet(key)              
    with lock:
        db.register_cp(cp_id, cp_loc, cp_prec, cp_token)
        db.update_cp_status(cp_id, "ACTIVADO")
        if cp_id in cps:
            cp=cps[cp_id]
            cp.localizacion=cp_loc
            cp.precio=cp_prec
            cp.estado="ACTIVADO"
            cp.ping=time.time()
            cp.clave_simetrica = cifrador
            cp.socket = conn
            COLA.put(f"CENTRAL: CP[{cp_id}] actualizado.")
        else:
            cps[cp_id]=ChargingPoint(cp_id, cp_loc, cp_prec, "ACTIVADO",cp_token)
            cps[cp_id].ping=time.time()
            cps[cp_id].clave_simetrica = cifrador
            cps[cp_id].socket = conn
            COLA.put(f"CENTRAL: CP[{cp_id}] registrado en la BD.")
    enviar(conn, addr, f"OK|{key_str}") # Enviar la clase simetrica

# Hilo que gestiona los mensajes de averia del CP.
def rec_koe(resto):
    try:
        cp_id=int(resto)
    except (TypeError, ValueError):
        COLA.put(f"CENTRAL: Mal formato de mensaje de KO.")
        return
    now=time.time()
    with lock:
        cp=cps.get(cp_id)
        if not cp:
            COLA.put(f"CENTRAL: Recibido KO de CP[{cp_id}] desconocido.")
            return
        cp.ping=now
        info=CARGAS_PENDIENTES.get(cp_id)
        driver_id=DRIVER_PENDIENTES.get(cp_id)
        if(cp.estado!="AVERIADO"):
            cp.estado="AVERIADO"
            db.update_cp_status(cp_id, "AVERIADO")
            COLA.put(f"CENTRAL: CP[{cp_id}] averiado.")
    if driver_id is not None:
        if info:
            cp_carga=info.get("cp_carga")
            cp_precio=info.get("cp_precio")
        else:
            cp_carga=0.0
            cp_precio=0.0
        producer.send("driver_commands", {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "status": "CHARGE_ABORTED",
            "cp_carga": round(cp_carga, 2),
            "price": cp_precio
        })
        producer.flush()
        COLA.put(f"CENTRAL: Suministro abortado en CP[{cp_id}]. Ticket enviado al Driver[{driver_id}].")
        READY_PENDIENTES.pop(cp_id, None)
        CARGAS_PENDIENTES.pop(cp_id, None)
        DRIVER_PENDIENTES.pop(cp_id, None)
            

# Hilo que gestiona los mensajes de renovacion de vida del CP.
def rec_vid(resto):
    if not resto:
        COLA.put("CENTRAL: Recibido renovacion de vida de un CP desconocido.")
        return
    try:
        cp_id=int(resto)
    except ValueError:
        COLA.put(f"CENTRAL: Mal formato de mensaje de renovacion de vida.")
        return
    now=time.time()
    with lock:
        cp=cps.get(cp_id)
        if not cp:
            COLA.put(f"CENTRAL: Recibido renovacion de vida de un CP[{cp_id}] desconocido.")
            return
        if cp.estado=="DESCONECTADO":
            COLA.put(f"CENTRAL: Recibido renovacion de vida de un CP[{cp_id}] desconectado.")
            return
        cp.ping=now
        if cp.estado=="AVERIADO":
            cp.estado="ACTIVADO"
            db.update_cp_status(cp_id, "ACTIVADO")
            COLA.put(f"CENTRAL: CP[{cp_id}] recuperado del estado averiado.")

# Hilo que gestiona la confirmacion de parada de un CP.
def rec_pak(cp_id):
    if not cp_id:
        COLA.put("CENTRAL: Recibido confirmacion de parada de un CP desconocido.")
        return
    with lock:
        cp=cps.get(cp_id)
        db.update_cp_status(cp_id, "PARADO")
        cp.estado="PARADO"
        PARADAS_PENDIENTES.pop(cp_id, None)
    COLA.put(f"CENTRAL: Parada de CP[{cp_id}] confirmada.")

# Hilo que gestiona la confirmacion de reanudado de un CP.
def rec_rak(cp_id):
    if not cp_id:
        COLA.put("CENTRAL: Recibido confirmacion de reanudado de un CP desconocido.")
        return
    with lock:
        cp=cps.get(cp_id)
        db.update_cp_status(cp_id, "ACTIVADO")
        cp.estado="ACTIVADO"
        REANUDOS_PENDIENTES.pop(cp_id, None)
    COLA.put(f"CENTRAL: Reanudado de CP[{cp_id}] confirmado.")

# Hilo que gestiona la solicitud de suministrar de un Driver.
def rec_rch(driver_id, cp_id):
    with lock:
        cp=cps.get(cp_id)
    if cp is None:
        producer.send("driver_commands", {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "status": "DESCONOCIDO"})
        producer.flush()
        COLA.put(f"CENTRAL: Solicitud de suministrar del Driver[{driver_id}] en el CP[{cp_id}] denegada porque el CP es desconocido.")
        return
    with lock:
        estado=cp.estado
        if (cp.localizacion in ALERTAS_CLIMA) and (estado=="ACTIVADO"):
            estado="PARADO"
    # Mensaje para Driver del estado actual del CP.
    producer.send("driver_commands", {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "status": f"{estado}"})
    producer.flush()

    if estado!="ACTIVADO":
        COLA.put(f"CENTRAL: Solicitud de suministrar del Driver[{driver_id}] en el CP[{cp_id}] denegada porque el CP esta {estado.lower()}.")
        return
    try:
        # Mensaje para CP.
        enviar_kafka_cifrado(cp_id,{
            "cp_id": cp_id,
            "driver_id": driver_id,
            "status": "START_CHARGE"
        })
    except Exception:
        COLA.put(f"CENTRAL: Error, no se pudo enviar mensaje de solicitud de suministrar a CP[{cp_id}].")
        return

    with lock:
        cp.estado="SUMINISTRANDO"
        db.update_cp_status(cp_id, "SUMINISTRANDO")
        READY_PENDIENTES[cp_id]=time.time()+TIMEOUT
        DRIVER_PENDIENTES[cp_id]=driver_id
    COLA.put(f"CENTRAL: Solicitud de suministrar del Driver[{driver_id}] en el CP[{cp_id}] aceptada.")
    COLA.put(f"CENTRAL: Esperando notificacion del CP.")

# Hilo que gestiona el mensaje de espera de enchufado del CP.
def rec_wait(cp_id):
    with lock:
        READY_PENDIENTES.pop(cp_id, None)
        driver_id=DRIVER_PENDIENTES.get(cp_id)
    COLA.put(f"CENTRAL: CP[{cp_id}] esperando a que el Driver[{driver_id}] enchufe.")

    producer.send("driver_commands", {
            "driver_id": driver_id,
            "cp_id": cp_id,
            "status": "ESPERANDO"})
    producer.flush()

def rec_ava(cp_id):
    with lock:
        cp=cps.get(cp_id)
        info=CARGAS_PENDIENTES.pop(cp_id, None)
        driver_id=DRIVER_PENDIENTES.pop(cp_id, None)
        READY_PENDIENTES.pop(cp_id, None)
        if cp and cp.estado=="SUMINISTRANDO":
            cp.estado="ACTIVADO"
            db.update_cp_status(cp_id, "ACTIVADO")
    if info:
        cp_carga=info.get("cp_carga")
        cp_precio=info.get("cp_precio")
    else:
        cp_carga=0.0
        cp_precio=0.0
    producer.send("driver_commands", {
        "driver_id": driver_id,
        "cp_id": cp_id,
        "status": "CHARGE_FINISHED",
        "cp_carga": cp_carga,
        "price": cp_precio
    })
    producer.flush()
    COLA.put(f"CENTRAL: CP[{cp_id}] ha finalizado su suministro. Ticket enviado al Driver[{driver_id}].")
    ciudad=None
    with lock:
        ciudad=PARADAS_CLIMA_PENDIENTES.pop(cp_id, None)
        if ciudad is not None:
            if ciudad in ALERTAS_CLIMA:
                CP_PARADOS_CLIMA[cp_id]=ciudad
            else:
                ciudad=None
    if ciudad is not None:
        threading.Thread(target=parar, args=(cp_id,), daemon=True).start()

def rec_cha(cp_id):
    with lock:
        cp=cps.get(cp_id)
        info=CARGAS_PENDIENTES.get(cp_id)
        driver_id=info.get("driver_id")
        cp_carga=info.get("cp_carga")
        cp_precio=info.get("cp_precio")
        if cp.estado!="SUMINISTRANDO" and cp.estado!="DESCONECTADO":
            cp.estado="SUMINISTRANDO"
            db.update_cp_status(cp_id, "SUMINISTRANDO")
            DRIVER_PENDIENTES[cp_id]=driver_id
    producer.send("driver_commands", {
        "driver_id": driver_id,
        "cp_id": cp_id,
        "status": "CHARGING",
        "cp_carga": cp_carga,
        "price": cp_precio
    })
    producer.flush()

# Hilo que gestiona el mensaje de peticion de CPs activos de Driver.
def rec_lis(driver_id):
    with lock:
        ids=[]
        for cp_id, cp in cps.items():
            if not cp:
                continue
            if cp.estado!="ACTIVADO":
                continue
            ids.append({
                "cp_id": cp_id,
                "precio": cp.precio,
                "localizacion": cp.localizacion
            })
    producer.send("driver_commands", {
        "driver_id": driver_id,
        "status": "CP_ACTIVOS",
        "lista": ids
    })
    producer.flush()
    COLA.put(f"CENTRAL: Enviada lista de CPs activos ({len(ids)}) al Driver[{driver_id}].")

# Hilo que gestiona la recepcion de mensajes via socket de la central.
def recepcion(conn, addr):
    try:
        while True:
            lon_msg=conn.recv(HEADER).decode(FORMAT)
            if not lon_msg:
                break
            lon_msg=int(lon_msg)
            raw=recv_all(conn, lon_msg)
            if not raw:
                break
            msg=raw.decode(FORMAT)
            cod=msg[:3]
            resto=msg[4:]
            # Mensaje de registro de CP.
            if(cod=="CPN"):
                rec_cpn(conn, addr, resto)
            # Mensaje de averia de CP.
            elif(cod=="KOE"):
                rec_koe(resto)
            # Mensaje de renovacion de vida de CP.
            elif(cod=="VID"):
                rec_vid(resto)
            else:
                COLA.put(f"CENTRAL: Codigo desconocido \"{cod}\"")
    except (socket.timeout, OSError):
        return None
    except Exception as e:
        COLA.put(f"CENTRAL: Error con {addr}: {e}")
    finally:
        conn.close()


def do_revocar(ids):
    for cp_id in ids:
        with lock:
            cp = cps.get(cp_id)
            if cp:
                cp.clave_simetrica = None 
                cp.estado = "DESCONECTADO"
                if hasattr(cp, 'socket') and cp.socket:
                    try:
                        cp.socket.close() 
                    except:
                        pass
                    cp.socket = None
                COLA.put(f"CENTRAL: Clave revocada y conexion cerrada para CP[{cp_id}].")

def do_parar(ids=None):
    if ids is None:
        with lock:
            ids=[]
            for i, c in cps.items():
                if c and (c.estado not in ("DESCONECTADO", "PARADO", "AVERIADO")):
                    ids.append(i)
    for i in ids:
        threading.Thread(target=parar, args=(i,), daemon=True).start()

# Hilo que para un CP.
def parar(cp_id):
    with lock:
        cp=cps.get(cp_id)
        if not cp:
            COLA.put(f"CENTRAL: Error al parar CP[{cp_id}], no existe en memoria."); return
        if cp.estado in ("PARADO", "AVERIADO", "DESCONECTADO"):
            COLA.put(f"CENTRAL: Error al parar CP[{cp_id}], actualmente esta {cp.estado}."); return
        if cp_id in PARADAS_PENDIENTES:
            COLA.put(f"CENTRAL: Error al parar CP[{cp_id}], ya tiene una parada pendiente."); return
    esperando_enchufe=False
    driver_id=None
    if((cp_id in DRIVER_PENDIENTES) and (cp_id not in READY_PENDIENTES) and (cp_id not in CARGAS_PENDIENTES)):
        esperando_enchufe=True
        driver_id=DRIVER_PENDIENTES.get(cp_id)
        READY_PENDIENTES.pop(cp_id, None)
        CARGAS_PENDIENTES.pop(cp_id, None)
        DRIVER_PENDIENTES.pop(cp_id, None)
    try:
        enviar_kafka_cifrado(cp_id, {"cp_id": cp_id, "status": "PARAR"})
    except Exception as e:
        COLA.put(f"CENTRAL: Error, no se pudo enviar mensaje de parada a CP[{cp_id}].")
        return
    with lock:
        PARADAS_PENDIENTES[cp_id]=time.time()+TIMEOUT
        COLA.put(f"CENTRAL: Parada de CP[{cp_id}] en curso.")
    if driver_id is not None:
        try:
            producer.send("driver_commands", {
                "driver_id": driver_id,
                "cp_id": cp_id,
                "status": "CHARGE_ABORTED",
                "cp_carga": 0.0,
                "price": 0.0
            })
            producer.flush()
            COLA.put(f"CENTRAL: Sesion abortada en CP[{cp_id}]. Ticket enviado a Driver[{driver_id}].")
        except Exception:
            COLA.put(f"CENTRAL: Error al enviar carga abortada a Driver[{driver_id}] para CP[{cp_id}].")

def do_reanudar(ids=None):
    if ids is None:
        with lock:
            ids=[]
            for i, c in cps.items():
                if c and (c.estado=="PARADO"):
                    ids.append(i)
    for i in ids:
        threading.Thread(target=reanudar, args=(i,), daemon=True).start()

# Hilo que reanuda un CP.
def reanudar(cp_id):
    with lock:
        cp=cps.get(cp_id)
        if not cp:
            COLA.put(f"CENTRAL: Error al reanudar CP[{cp_id}], no existe en memoria.");
            return
        if cp.estado!="PARADO":
            COLA.put(f"CENTRAL: Error al reanudar CP[{cp_id}], no estaba parado.");
            return
    try:
       enviar_kafka_cifrado(cp_id, {"cp_id": cp_id, "status": "REANUDAR"})
    except Exception as e:
        COLA.put(f"CENTRAL: Error, no se pudo enviar mensaje de reanudar a CP[{cp_id}].")
        return
    with lock:
        REANUDOS_PENDIENTES[cp_id]=time.time()+TIMEOUT
        COLA.put(f"CENTRAL: Reanudación de CP[{cp_id}] en curso.")

# Hilo que gestiona la caducidad de todos los mensajes.
def comprobador_vida():
    while True:
        time.sleep(1)
        now=time.time()
        with lock:
            for cp_id, cp in list(cps.items()):
                if(now-cp.ping>TIMEOUT) and (cp.estado!="DESCONECTADO"):
                    db.update_cp_status(cp_id, "DESCONECTADO")
                    cp.estado="DESCONECTADO"
                    info=CARGAS_PENDIENTES.pop(cp_id, None)
                    driver_id = DRIVER_PENDIENTES.pop(cp_id, None)
                    READY_PENDIENTES.pop(cp_id, None)
                    if driver_id is not None:
                        if info:
                            try:
                                cp_carga = round(float(info.get("cp_carga", 0.0)), 2)
                            except Exception:
                                cp_carga = 0.0
                            try:
                                cp_precio = float(info.get("cp_precio", 0.0))
                            except Exception:
                                cp_precio = 0.0
                        else:
                            cp_carga = 0.0
                            cp_precio = 0.0

                        producer.send("driver_commands", {
                            "driver_id": driver_id,
                            "cp_id": cp_id,
                            "status": "CHARGE_ABORTED",
                            "cp_carga": cp_carga,
                            "price": cp_precio
                        })
                        producer.flush()
                        COLA.put(f"CENTRAL: Sesión abortada por desconexión en CP[{cp_id}]. Ticket enviado a Driver[{driver_id}].")
                    COLA.put(f"CENTRAL: CP[{cp_id}] desconectado por falta de vida.")
                pendiente_p=PARADAS_PENDIENTES.get(cp_id)
                if(pendiente_p and now>pendiente_p):
                    PARADAS_PENDIENTES.pop(cp_id, None)
                    COLA.put(f"CENTRAL: Parada de CP[{cp_id}] cancelada por falta de confirmacion.")
                pendiente_r=REANUDOS_PENDIENTES.get(cp_id)
                if(pendiente_r and now>pendiente_r):
                    REANUDOS_PENDIENTES.pop(cp_id, None)
                    COLA.put(f"CENTRAL: Reanudacion de CP[{cp_id}] cancelada por falta de confirmacion.")
                pendiente_rea=READY_PENDIENTES.get(cp_id)
                if(pendiente_rea and now>pendiente_rea):
                    READY_PENDIENTES.pop(cp_id, None)
                    driver_id=DRIVER_PENDIENTES.pop(cp_id, None)
                    if driver_id is not None:
                        producer.send("driver_commands", {
                                "driver_id": driver_id,
                                "cp_id": cp_id,
                                "status": "CADUCADO"})
                        producer.flush()
                        cp.estado = "ACTIVADO"
                        db.update_cp_status(cp_id, "ACTIVADO")

def leer_kafka():
    while True:
        try:
            kafka_msg=consumer.poll(timeout_ms=100)
            if not kafka_msg:
                continue
            for messages in kafka_msg.values():
                for m in messages:
                    data=m.value
                    data_final = None
                    # Cola de CPs
                    if m.topic=="cp_status":
                        cp_id=data.get("cp_id")
                        if cp_id is None:
                            continue
                        with lock:
                            cp=cps.get(cp_id)
                        if not cp:
                            COLA.put(f"CENTRAL: Recibido mensaje del CP[{cp_id}] desconocido.")
                            continue
                        if "payload_cifrado" in data: #el cifrado de engine
                            if cp.clave_simetrica:
                                try:
                                    encrypted_bytes = data["payload_cifrado"].encode('utf-8')
                                    decrypted_str = cp.clave_simetrica.decrypt(encrypted_bytes).decode('utf-8')
                                    data_final = json.loads(decrypted_str) 
                                except Exception as e:
                                    COLA.put(f"CENTRAL: Error descifrado CP[{cp_id}]: {e}")
                                    continue
                            else:
                                COLA.put(f"CENTRAL: CP[{cp_id}] envía cifrado pero no tengo clave.")
                                continue

                        if not data_final:
                            continue
                        estado=data_final.get("status", "")
                        # Confirmacion de parada.
                        if estado=="RAK":
                            threading.Thread(target=rec_rak, args=(cp_id,), daemon=True).start()
                        # Confirmacion de reanudado.
                        elif estado=="PAK":
                            threading.Thread(target=rec_pak, args=(cp_id,), daemon=True).start()
                        # CP en espera a ser enchufado.
                        elif estado=="WAIT_CHARGE":
                            threading.Thread(target=rec_wait, args=(cp_id,), daemon=True).start()
                        elif estado=="CHARGING":
                            driver_id=data_final.get("driver_id")
                            with lock:
                                if not driver_id in drivers:
                                    db.register_driver(driver_id)
                                    drivers.append(driver_id)
                                # Detectar si es el primer mensaje de suministrar.
                                primer=(cp_id not in CARGAS_PENDIENTES)
                                CARGAS_PENDIENTES[cp_id]={
                                    "cp_carga": data_final.get("cp_carga"),
                                    "cp_precio": data_final.get("cp_precio"),
                                    "driver_id": driver_id
                                }
                            if primer:
                                COLA.put(f"CENTRAL: CP[{cp_id}] suministrando a Driver[{driver_id}].")
                            threading.Thread(target=rec_cha, args=(cp_id,), daemon=True).start()
                        # CP finalizo el suministrar.
                        elif estado=="AVAILABLE":
                            threading.Thread(target=rec_ava, args=(cp_id,), daemon=True).start()
                        elif estado=="MANUAL_CHARGE":
                            driver_id=data_final.get("driver_id")
                            with lock:
                                if driver_id not in drivers:
                                    db.register_driver(driver_id)
                                    drivers.append(driver_id)
                            COLA.put(f"CENTRAL: CP[{cp_id}] solicitó carga manual para Driver[{driver_id}].")
                            threading.Thread(target=rec_rch, args=(driver_id, cp_id,), daemon=True).start()   
                    # Cola de Drivers.
                    elif m.topic=="driver_requests":
                        driver_id=data.get("driver_id")                     
                        cp_id=data.get("cp_id")                     
                        estado=data.get("status")                      
                        if driver_id is None:
                            continue
                        with lock:
                            if not driver_id in drivers:
                                db.register_driver(driver_id)
                                drivers.append(driver_id)
                            if estado=="REQUEST_CHARGE":
                                threading.Thread(target=rec_rch, args=(driver_id, cp_id,), daemon=True).start()
                            elif estado=="LIST_CP":
                                threading.Thread(target=rec_lis, args=(driver_id,), daemon=True).start()
        except Exception as e:
            COLA.put(f"CENTRAL: Error en leer kafka")
            time.sleep(0.5)

# Funcion que pide el clima al API_Central.
def pedir_clima():
    try:
        return db.getOpenWeather()
    except Exception:
        return None

def aplicar_alerta(ciudad, temp):
    try:
        filas=db.getCPsByCity(ciudad)
    except Exception as e:
        COLA.put(f"CENTRAL: Error al leer CPs para la ciudad {ciudad}: {e}")
        filas=[]
    nuevos=[]
    with lock:
        ALERTAS_CLIMA[ciudad]=temp
        for fila in filas:
            i=fila["id"]
            cp=cps.get(i)
            if cp is None:
                continue
            if cp.localizacion != ciudad:
                cp.localizacion = ciudad
            if cp.estado in ("AVERIADO","DESCONECTADO"):
                continue
            if CP_PARADOS_CLIMA.get(i)==ciudad:
                continue
            nuevos.append(i)
    if len(nuevos)==0:
        return

    COLA.put(f"CENTRAL: Alerta de temperatura en {ciudad}. Temp. {temp}.")
    for i in nuevos:
        with lock:
            # Si está en carga, se marca para parar al terminar.
            if (i in CARGAS_PENDIENTES):
                PARADAS_CLIMA_PENDIENTES[i]=ciudad
                CP_PARADOS_CLIMA[i]=ciudad
                continue
            # Si está libre, se para directamente.
            CP_PARADOS_CLIMA[i]=ciudad
        threading.Thread(target=parar, args=(i,), daemon=True).start()

def cancelar_alerta_clima(ciudad):
    with lock:
        if ciudad in ALERTAS_CLIMA:
            ALERTAS_CLIMA.pop(ciudad, None)
        for i in list(PARADAS_CLIMA_PENDIENTES.keys()):
            if (PARADAS_CLIMA_PENDIENTES.get(i)==ciudad):
                PARADAS_CLIMA_PENDIENTES.pop(i, None)
        ids=[]
        for i in list(CP_PARADOS_CLIMA.keys()):
            if (CP_PARADOS_CLIMA.get(i)!=ciudad):
                continue
            cp=cps.get(i)
            if cp and (cp.estado=="PARADO"):
                ids.append(i)
                CP_PARADOS_CLIMA.pop(i, None)
    for i in ids:
        threading.Thread(target=reanudar, args=(i,), daemon=True).start()

def comprobador_clima():
    fallo=False
    while True:
        time.sleep(2)
        lista=pedir_clima()
        if lista is None:
            if fallo==False:
                fallo=True
                COLA.put("CENTRAL: No se pudo acceder al clima.")
            continue
        if fallo==True:
            fallo=False
            COLA.put("CENTRAL: Clima recuperado.")
        nuevas_alertas={}
        for i in lista:
            if not isinstance(i, dict):
                continue
            ciudad=str(i.get("ciudad","")).strip()
            temp=i.get("temp", None)
            if (ciudad=="") or (temp is None):
                continue
            temp=float(temp)
            if temp<0:
                nuevas_alertas[ciudad]=temp
        for ciudad, temp in nuevas_alertas.items():
            aplicar_alerta(ciudad, temp)
        desactivar=[]
        with lock:
            for i in list(ALERTAS_CLIMA.keys()):
                if i not in nuevas_alertas:
                    desactivar.append(i)
        for i in desactivar:
            cancelar_alerta_clima(i)


# Lanzamos los hilos.
threading.Thread(target=qprint, daemon=True).start()
threading.Thread(target=comprobador_vida, daemon=True).start()
threading.Thread(target=leer_kafka, daemon=True).start()
threading.Thread(target=comprobador_clima, daemon=True).start()
# Configura el socket del servidor.
PUERTO=int(sys.argv[1])
server=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("0.0.0.0", PUERTO))
server.listen()
print("CENTRAL lista.")
if os.getenv("TUI_DELAY", "1")=="1":
    input("Pulsa ENTER para abrir la interfaz de la central.")
threading.Thread(target=interfaz, daemon=True).start()

while(True):
    conn, addr=server.accept()
    thread=threading.Thread(target=recepcion, args=(conn, addr)).start()