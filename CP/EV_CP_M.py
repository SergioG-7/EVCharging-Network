#Monitor del CP
import socket,ssl,requests
import threading
import time
import sys,os
from queue import Queue
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
HEADER = 10
FORMAT = 'utf-8'
COLA = Queue()
LOG = open("monitor.log", "a", buffering=1, encoding=FORMAT)

ENGINE_IP = sys.argv[1]
ENGINE_PORT = int(sys.argv[2])
CENTRAL_IP = sys.argv[3]
CENTRAL_PORT = int(sys.argv[4])
CP_ID = int(sys.argv[5]) 
CP_LOCATION = sys.argv[6]
CP_PRICE = float(sys.argv[7])
lock = threading.Lock()
REGISTRY_IP = sys.argv[8]
REGISTRY_URL = f'https://{REGISTRY_IP}:8443/register'

# ---- ESTRUCTURA DE ESTADO ----
class EstadoMonitor:
    def __init__(self):
        self.engine_conn = None
        self.central_conn = None
        self.averiado = False
        self.engine_caido_notificado = False
        self.central_caido_notificado = False
        self.registrado = False 
        self.clave_simetrica = None
        self.token_registry = None

estado = EstadoMonitor()
# --- Función para enviar mensajes a la Central ---
def qprint():
    while True:
        msg = COLA.get()
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"[{timestamp}] {msg}"
            LOG.write(log_entry + "\n")
        finally:
            COLA.task_done()

def send(conn,msg):
    contenido = msg.encode(FORMAT)
    cabecera = (str(len(contenido))).encode(FORMAT)
    cabecera+= b' '*(HEADER-len(cabecera))
    conn.sendall(cabecera+contenido)

def recvall(sock, n):
    data = b''
    sock.settimeout(5)
    while len(data) < n:
        try:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data += packet
        except socket.timeout:
            COLA.put(f"[MONITOR {CP_ID}] Timeout en recvall")
            return None
    return data

# --- Registrarse ---
def registrar_cp():
    COLA.put(f"[MONITOR {CP_ID}] Solicitando registro API REST a {REGISTRY_URL}...")
    
    payload = {'id': CP_ID, 'location': CP_LOCATION}
    
    try:

        response = requests.post(REGISTRY_URL, json=payload, verify=False, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'OK':
                token = data.get('token')
                
                with lock:
                    estado.registrado = True
                    estado.token_registry = token
                
                COLA.put(f"[MONITOR {CP_ID}] REGISTRO EXITOSO. Token: {token}")
                return True
            else:
                COLA.put(f"[MONITOR {CP_ID}] Registry error: {data.get('message')}")
        else:
            COLA.put(f"[MONITOR {CP_ID}] Error HTTP: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        COLA.put(f"[MONITOR {CP_ID}] Error: No se puede conectar al Registry (HTTPS).")
    except Exception as e:
        COLA.put(f"[MONITOR {CP_ID}] Excepción registro: {e}")
    return False


def baja_cp():
    COLA.put(f"[MONITOR {CP_ID}] Solicitando BAJA en Registry...")
    try:
        
        response = requests.delete(f"{REGISTRY_URL}/{CP_ID}", verify=False, timeout=5)
        
        if response.status_code == 200:
            COLA.put(f"[MONITOR {CP_ID}] Baja realizada correctamente.")
            with lock:
                estado.registrado = False
                estado.token_registry = None
                estado.clave_simetrica = None
                if estado.central_conn: 
                    try: 
                        estado.central_conn.close()
                    except: 
                        pass
                    estado.central_conn = None
                if estado.engine_conn: 
                    try: 
                        estado.engine_conn.close()
                    except: 
                        pass
                    estado.engine_conn = None
            return True
        else:
            COLA.put(f"[MONITOR {CP_ID}] Error al dar de baja: {response.status_code} - {response.text}")
            
    except Exception as e:
        COLA.put(f"[MONITOR {CP_ID}] Error al dar de baja: {e}")
    return False
# --- Conexión al engine ---
def conectar_engine():
    try:
        with lock:
            estado.engine_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            estado.engine_conn.settimeout(2)
            estado.engine_conn.connect((ENGINE_IP, ENGINE_PORT))
            COLA.put(f"[MONITOR {CP_ID}] Conectado con el Engine en {ENGINE_IP}:{ENGINE_PORT}")
            send(estado.engine_conn, f"ID|{CP_ID}|{estado.clave_simetrica}") #Le pasamos la clave token
            COLA.put(f"[MONITOR {CP_ID}] ID enviada a ENGINE ({CP_ID})")
            estado.averiado = False
            estado.engine_caido_notificado = False
            return estado.engine_conn
    except Exception as e:
        with lock:
            if not estado.averiado:
                COLA.put(f"[MONITOR {CP_ID}] No se pudo conectar con el Engine ({e}). Reintentando.")
                estado.averiado = True
        time.sleep(1)


# --- Conexión a la central ---
def conectar_central():
    conectado_central = False
    while True:
        try:
            if not estado.registrado:
                time.sleep(2)
                continue
            with lock:
                estado.central_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                estado.central_conn.connect((CENTRAL_IP, CENTRAL_PORT))
                if not conectado_central:
                    COLA.put(f"[MONITOR {CP_ID}] Conectado con la central en {CENTRAL_IP}:{CENTRAL_PORT}")
                    conectado_central = True
                send(estado.central_conn, f"CPN|{CP_ID}|{CP_LOCATION}|{CP_PRICE}|{estado.token_registry}") #token
            resp_len = estado.central_conn.recv(HEADER).decode(FORMAT)
            if resp_len:
                resp = recvall(estado.central_conn, int(resp_len)).decode(FORMAT)
                if resp.startswith("OK"):
                    parts = resp.split("|")
                    estado.clave_simetrica = parts[1]
                    COLA.put(f"[MONITOR {CP_ID}] CP registrado correctamente en central y clave simetrica recibida")
                    estado.central_caido_notificado = False
                    if estado.engine_conn:
                                try:
                                    send(estado.engine_conn, f"ID|{CP_ID}|{estado.clave_simetrica}")
                                    COLA.put(f"[MONITOR {CP_ID}] Clave actualizada enviada al Engine.")
                                except Exception as e:
                                    COLA.put(f"[MONITOR] Error propagando clave al Engine: {e}")
                    return estado.central_conn
                else:
                    COLA.put(f"[MONITOR {CP_ID}] Central respondió con error: {resp}")
        except Exception as e:
            with lock:
                if not estado.central_caido_notificado:
                    COLA.put(f"[MONITOR {CP_ID}] No se pudo conectar con la central ({e}). Reintentando.")
                estado.central_caido_notificado = True
            time.sleep(1)
  
def safe_send(destino, msg):
    if isinstance(destino, str):
        conn = getattr(estado, destino)
        attr_name = destino
    else:
        conn = destino
        if conn == estado.central_conn:
            attr_name = "central_conn"
        elif conn == estado.engine_conn:
            attr_name = "engine_conn"
        else:
            attr_name = None

    if conn is None:
        if attr_name == "central_conn" and not estado.central_caido_notificado:
            COLA.put(f"[MONITOR {CP_ID}] Error al enviar a central_conn: Socket nulo")
            estado.central_caido_notificado = True
        elif attr_name == "engine_conn" and not estado.engine_caido_notificado:
            COLA.put(f"[MONITOR {CP_ID}] Error al enviar a engine_conn: Socket nulo")
            estado.engine_caido_notificado = True
        return

    try:
        send(conn, msg)
    except Exception as e:
        COLA.put(f"[MONITOR {CP_ID}] Error al enviar a {attr_name or conn}: {e}")
        try:
            conn.close()
        except:
            pass
        if attr_name:
            setattr(estado, attr_name, None)
        if attr_name == "central_conn":
            estado.central_caido_notificado = False



# --- Bucle principal ---
def bucle_monitor():
    while not estado.registrado: #Solo intenta conectar si se ha registrado
        time.sleep(0.1)
    
    estado.central_conn = conectar_central()
    estado.engine_conn = conectar_engine()
    while True:
        if not estado.registrado:
            time.sleep(1)
            continue
        if estado.engine_conn is None: 
            if not estado.engine_caido_notificado:
                COLA.put(f"[MONITOR {CP_ID}] Intentando reconectar con Engine...")
                estado.engine_caido_notificado = True
                safe_send(estado.central_conn, f"KOE|{CP_ID}") 
            estado.engine_conn = conectar_engine() 
            if estado.engine_conn:
                COLA.put(f"[MONITOR {CP_ID}] Engine reconectado. Re-registrando CP {CP_ID} en Central.")
                safe_send(estado.central_conn, f"CPN|{CP_ID}|{CP_LOCATION}|{CP_PRICE}")
                estado.engine_caido_notificado = False
            continue
        if estado.central_conn is None:
            if not estado.central_caido_notificado:
                COLA.put(f"[MONITOR {CP_ID}] Intentando reconectar con Central...")
                estado.central_caido_notificado = True
            estado.central_conn = conectar_central()
            if estado.central_conn:
                COLA.put(f"[MONITOR {CP_ID}] Central reconectado.")
                estado.central_caido_notificado = False
        try:
            if estado.engine_conn is not None:
                
                safe_send(estado.engine_conn, "PING")
                try:
                    resp_len = estado.engine_conn.recv(HEADER).decode(FORMAT)
                except Exception as e:
                    COLA.put(f"[MONITOR {CP_ID}] Engine caído durante recv: {e}")
                    if estado.engine_conn is not None:
                        try: estado.engine_conn.close()
                        except: pass
                    estado.engine_conn = None
                    estado.engine_caido_notificado = True
                    safe_send(estado.central_conn, f"KOE|{CP_ID}")
                    continue 

                if resp_len:
                    resp = recvall(estado.engine_conn, int(resp_len)).decode(FORMAT)
                    if resp == "OK":
                        if not estado.averiado:
                            safe_send(estado.central_conn, f"VID|{CP_ID}")  
                        elif estado.engine_caido_notificado:
                            COLA.put(f"[MONITOR {CP_ID}] Engine recuperado, enviando VID a central.")
                            estado.engine_caido_notificado = False
                            estado.averiado = False
                            safe_send(estado.central_conn, f"VID|{CP_ID}")
                     
                    elif resp.startswith("STATUS"):
                        parts = resp.split("|")
                        status = parts[2]
                        COLA.put(f"[MONITOR {CP_ID}] Estado CP {CP_ID} = {status}")
                    else:
                        if not estado.engine_caido_notificado:
                            COLA.put(f"[MONITOR {CP_ID}] Engine averiado, enviando KOE a central.")
                        safe_send(estado.central_conn, f"KOE|{CP_ID}")  
                        estado.engine_caido_notificado = True
                        estado.averiado = True
                else:
                    if not estado.engine_caido_notificado:
                        COLA.put(f"[MONITOR {CP_ID}] Engine caído. Esperando reconexión...")
                        estado.engine_caido_notificado = True
                    safe_send(estado.central_conn, f"KOE|{CP_ID}")  

        except Exception as e:
            COLA.put(f"[MONITOR {CP_ID}] Error comunicando con Engine: {e}")
            if estado.engine_conn is not None and estado.central_conn is not None:
                safe_send(estado.central_conn, f"KOE|{CP_ID}")  
            estado.engine_conn = None
            estado.engine_caido_notificado = True

        time.sleep(1)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def menu_principal():
    while True:
        print(f"\n[MONITOR {CP_ID}] Menu:")
        if estado.registrado:
            print(f"[Estado: REGISTRADO - Token: {estado.token_registry}]")
        else:
            print(f"[Estado: NO REGISTRADO]")
        print("1 - Registrarse") 
        print("2 - Ver estado")
        print("3 - Darse de baja")
        opcion = input("Tu: ").strip()
        clear_screen()
        if opcion == "1":
            if estado.registrado:
                print("Ya estas registrado.")
            else:
                registrar_cp()
        elif opcion == "2":
            print(f"Conexión Engine: {estado.engine_conn is not None}")
            print(f"Conexión Central: {estado.central_conn is not None}")
        elif opcion == "3":
            if estado.registrado:
                baja_cp()
            else:
                print("No estabas registrado")

# ---- MAIN ----
def main():
    time.sleep(3)
    threading.Thread(target=qprint, daemon=True).start()
    threading.Thread(target=bucle_monitor, daemon=True).start()
    menu_principal()  


if __name__ == "__main__":
    main()


