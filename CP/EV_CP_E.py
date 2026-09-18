import socket, sys, time, random, threading, json,os
from queue import Queue
from unicodedata import digit
from kafka import KafkaProducer, KafkaConsumer
from cryptography.fernet import Fernet
from datetime import datetime

HEADER = 10
FORMAT = 'utf-8'
COLA = Queue()
LOG = open("engine.log", "a", buffering=1, encoding=FORMAT)
lock = threading.Lock()


ENGINE_IP = sys.argv[1]
ENGINE_PORT = int(sys.argv[2])
KAFKA_HOST = sys.argv[3]
KAFKA_PORT = int(sys.argv[4])

# ---- ESTRUCTURA DE ESTADO ----
class EstadoEngine:
    def __init__(self):
        self.monitor_server = None
        self.monitor_conn = None
        self.CP_ID = None
        self.precio =  float(sys.argv[5])
        self.charging = False
        self.force_fault = False
        self.parado = False
        self.energy = 0.0
        self.driver = None
        self.producer = None
        self.consumer = None
        self.wait_enchufado = False
        self.clave_simetrica = None

estado = EstadoEngine()

# ---- FUNCIONES AUXILIARES ----
def qprint():
    while True:
        msg = COLA.get()
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"[{timestamp}] {msg}"
            LOG.write(log_entry + "\n")
        finally:
            COLA.task_done()

def send(conn, msg):
    contenido = msg.encode(FORMAT)
    cabecera = (str(len(contenido))).encode(FORMAT)
    cabecera += b' ' * (HEADER - len(cabecera))
    conn.sendall(cabecera + contenido)

def recvall(sock, n):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def enviar_kafka_cifrado(topic, data_dict):
    if estado.clave_simetrica is not None:
        try:
            json_str = json.dumps(data_dict)
            token_cifrado = estado.clave_simetrica.encrypt(json_str.encode('utf-8'))
            
            mensaje_final = {
                "cp_id": estado.CP_ID,
                "payload_cifrado": token_cifrado.decode('utf-8') 
            }
            estado.producer.send(topic, mensaje_final)
            
        except Exception as e:
            COLA.put(f"[ENGINE {estado.CP_ID}] Error cifrando mensaje: {e}")
    else:
        estado.producer.send(topic, data_dict) #por si falla algo
# ---- MONITOR ----
def hilo_monitor():
    with lock:
        estado.monitor_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        estado.monitor_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        estado.monitor_server.bind((ENGINE_IP, ENGINE_PORT))
        estado.monitor_server.listen(1)
    COLA.put(f"[ENGINE] Escuchando Monitor en {ENGINE_IP}:{ENGINE_PORT}")

    while True:
        conn, addr = estado.monitor_server.accept()
        with lock:
            estado.monitor_conn = conn
        COLA.put(f"[ENGINE] Monitor conectado desde {addr}")
        threading.Thread(target=escuchar_monitor, daemon=True).start()

def escuchar_monitor():
    while True:
        try:
            header = estado.monitor_conn.recv(HEADER)
            if not header:
                COLA.put(f"[ENGINE {estado.CP_ID}] Monitor desconectado.")
                reiniciar_estado()
                break
            try:
                msg_len = int(header.decode(FORMAT).strip())
            except ValueError:
                COLA.put(f"[ENGINE {estado.CP_ID}] Cabecera inválida recibida: {header}")
                continue
            data = recvall(estado.monitor_conn, msg_len)
            if data is None:
                COLA.put(f"[ENGINE {estado.CP_ID}] Monitor desconectado durante recepción.")
                estado.monitor_conn = None
                break
            msg = data.decode(FORMAT)

            if msg.startswith("ID|"):
                with lock:
                    estado.CP_ID = int(msg.split("|")[1])
                try:
                    estado.clave_simetrica = Fernet((msg.split("|")[2]).encode('utf-8'))
                    COLA.put(f"[ENGINE] Clave de cifrado recibida y activa.")
                except Exception as e:
                        COLA.put(f"[ENGINE] Error formato clave: {e}")
                COLA.put(f"[ENGINE] CP_ID = {estado.CP_ID}")
            elif msg == "PING":
                with lock:
                    reply = "KO" if estado.force_fault else "OK"
                    send(estado.monitor_conn,reply)
            else:
                COLA.put(f"[ENGINE {estado.CP_ID}] Mensaje desconocido desde monitor: {msg}")

        except Exception as e:
            COLA.put(f"[ENGINE {estado.CP_ID}] Error escuchando monitor: {e}")
            reiniciar_estado()
            break

# ---- KAFKA ----
def hilo_kafka():
    while True:
        with lock:
            if estado.CP_ID:
                break
        time.sleep(0.1)

    
    estado.producer = KafkaProducer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        value_serializer=lambda v: json.dumps(v).encode(FORMAT)
    )
    estado.consumer = KafkaConsumer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        value_deserializer=lambda v: json.loads(v.decode(FORMAT)),
        group_id=f"engine_group_{estado.CP_ID}",
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    estado.consumer.subscribe(topics=["cp_commands"])
    COLA.put(f"[ENGINE {estado.CP_ID}] Conectado con Kafka.")

    while True:
        try:
            kafka_msgs = estado.consumer.poll(timeout_ms=500)
            for messages in kafka_msgs.values():
                for msg in messages:
                    data = msg.value
                    data_final = None
                    with lock:
                        if data.get("cp_id") != estado.CP_ID:
                            continue
                        if "payload_cifrado" in data:
                            if estado.clave_simetrica:
                                try:
                                    enc_bytes = data["payload_cifrado"].encode('utf-8')
                                    dec_str = estado.clave_simetrica.decrypt(enc_bytes).decode('utf-8')
                                    data_final = json.loads(dec_str)
                                except Exception as e:
                                    COLA.put(f"[ENGINE {estado.CP_ID}] Error descifrando comando: {e}")
                                    continue
                            else:
                                COLA.put(f"[ENGINE {estado.CP_ID}] Recibido comando cifrado pero no tengo clave.")
                                continue

                        if data_final:
                            handle_kafka_msg(data_final)
        except Exception as e:
            COLA.put(f"[ENGINE {estado.CP_ID}] Error Kafka: {e}")

def handle_kafka_msg(data):
    action = data.get("status")
    driver = data.get("driver_id")

    if action == "PARAR":
        estado.charging = False
        estado.wait_enchufado = False
        estado.parado = True
        estado.driver = None
        estado.energy = 0.0
        enviar_kafka_cifrado("cp_status", {"cp_id": estado.CP_ID, "status": "PAK"})
        send(estado.monitor_conn, f"STATUS|{estado.CP_ID}|CP_PARADO")
        COLA.put(f"[ENGINE {estado.CP_ID}] Parado.")

    elif action == "REANUDAR":
        estado.parado = False
        enviar_kafka_cifrado("cp_status", {"cp_id": estado.CP_ID, "status": "RAK"})
        send(estado.monitor_conn, f"STATUS|{estado.CP_ID}|CP_REANUDADO")
        COLA.put(f"[ENGINE {estado.CP_ID}] Reanudado.")

    elif action == "START_CHARGE":
        estado.driver = driver
        estado.energy = 0.0
        enviar_kafka_cifrado("cp_status", {"cp_id": estado.CP_ID, "status": "WAIT_CHARGE"})
        estado.wait_enchufado = True
        COLA.put(f"[ENGINE {estado.CP_ID}] Esperando enchufado para iniciar la carga.")


    estado.producer.flush()

def hilo_carga():
    while True:
        with lock:
            if not estado.charging and not estado.force_fault: 
                enviar_kafka_cifrado("cp_status", {"cp_id": estado.CP_ID,"status": "AVAILABLE"})
                break
            if estado.force_fault or not estado.monitor_conn:
                estado.charging = False
                break
            estado.energy += round(random.uniform(0.1, 0.5), 2) 
            COLA.put(f"[ENGINE {estado.CP_ID}] Energia acumulada: {estado.energy:.2f} kWh")
            enviar_kafka_cifrado("cp_status", {
                "cp_id": estado.CP_ID, 
                "driver_id": estado.driver,
                "cp_carga": estado.energy,
                "cp_precio": round(estado.energy*estado.precio,2),
                "status": "CHARGING"
            })
        time.sleep(1)  

def reiniciar_estado():
    with lock:
        estado.CP_ID = None         
        estado.monitor_conn = None   
        estado.charging = False     
        estado.driver = None
        estado.energy = 0.0
        estado.clave_simetrica = None 
        estado.wait_enchufado = False
    COLA.put("[ENGINE] Estado reiniciado por desconexión del Monitor.")

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
# ---- MENÚ ----
def menu_principal():
    while True:
        if estado.CP_ID:
            break
        time.sleep(0.1)


    while True:
        print(f"\n[ENGINE {estado.CP_ID}] Menu:")
        print("1 - Carga manual") 
        print("2 - Ver estado")
        print("3 - Enchufado")
        print("4 - Desenchufado")
        print("f - Forzar/Quitar averia") 
        opcion = input("Tu: ").strip()
        clear_screen()
        with lock:
            if estado.monitor_conn is None and not estado.charging:
                if opcion not in ["2"]:
                    print("El Monitor esta caido: solo puedes usar '2' (ver estado).")
                    continue
            elif estado.parado or estado.force_fault:
                if opcion not in ["2","f"]:
                    print("El CP esta parado/averiado: solo puedes usar '2' (ver estado) o 'f' (forzar fallo).")
                    continue
            elif estado.charging:
                if opcion not in ["2", "4", "f"]:
                    print("El CP esta cargando: solo puedes usar '4' (desenchufar), '2' (ver estado) o 'f' (forzar fallo).")
                    continue
            elif estado.wait_enchufado:
                if opcion not in ["2", "3", "f"]:
                    print("El CP esta esperando enchufado: solo puedes usar '3' (enchufar), '2' (ver estado) o 'f' (forzar fallo).")
                    continue
            elif opcion == "3" and estado.charging:
                print("Ya estas cargando, no puedes volver a enchufar.")
                continue

        #Acciones
        if opcion == "1":
            print(f"[ENGINE {estado.CP_ID}] Introduce el driver al que conectarse: ")
            try:
                repostar = int(input("Tu: ").strip())  
                estado.driver = repostar
                print(f"[ENGINE {estado.CP_ID}] Carga manual solicitada para driver {estado.driver}.")
                enviar_kafka_cifrado("cp_status", {
                        "cp_id": estado.CP_ID, 
                        "driver_id": estado.driver, 
                        "status": "MANUAL_CHARGE"
                    })
            except ValueError:
                print("Introduce un numero valido de Driver.")
                estado.driver = None
                
        elif opcion == "2":
            with lock:
                print(f"\n--- Estado ---")
                print(f"CP_ID: {estado.CP_ID}")
                print(f"Cargando: {estado.charging}")
                print(f"Parado: {estado.parado}")
                print(f"Averiado: {estado.force_fault}")
                print(f"Energia: {estado.energy:.2f}")
                print(f"Driver: {estado.driver}")
        
        elif opcion == "3":
            with lock:
                if estado.wait_enchufado:
                    estado.charging = True
                    estado.wait_enchufado = False
                    enviar_kafka_cifrado("cp_status", {
                        "cp_id": estado.CP_ID, 
                        "driver_id": estado.driver,
                        "cp_carga": estado.energy,
                        "cp_precio": round(estado.energy*estado.precio,2),
                        "status": "CHARGING"
                    })
                    send(estado.monitor_conn, f"STATUS|{estado.CP_ID}|CHARGING")
                    print(f"[ENGINE {estado.CP_ID}] Carga iniciada tras enchufado.")
                    threading.Thread(target=hilo_carga, daemon=True).start()
                else:
                    print("No se esta esperando enchufado.")
        elif opcion == "4":
            with lock:
                if estado.charging:
                    estado.charging = False
                    estado.wait_enchufado = False
                    send(estado.monitor_conn, f"STATUS|{estado.CP_ID}|CHARGE_FINISHED")
                    print(f"[ENGINE {estado.CP_ID}] Carga finalizada tras desenchufado.") 
                    estado.driver = None
                    estado.energy = 0.0
                else:
                    print("No se esta cargando.")
            
        elif opcion == "f":
            with lock:
                estado.force_fault = not estado.force_fault
                if estado.force_fault:
                    if estado.charging:
                        estado.charging = False
                        estado.driver = None
                        estado.energy = 0.0
                        print(f"[ENGINE {estado.CP_ID}] Carga detenida por averia forzada.")
                    else:
                        print(f"[ENGINE {estado.CP_ID}] Averia forzada ACTIVADA.")

                else:
                    print(f"[ENGINE {estado.CP_ID}] Averia forzada DESACTIVADA.")
                    estado.parado = False
                    estado.wait_enchufado = False

        else:
            print("Opción invalida.")

# ---- MAIN ----
def main():
    time.sleep(3)
    threading.Thread(target=qprint, daemon=True).start()
    threading.Thread(target=hilo_monitor, daemon=True).start()
    threading.Thread(target=hilo_kafka, daemon=True).start()
    threading.Thread(target=menu_principal, daemon=True).start()  
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
