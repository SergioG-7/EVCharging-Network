# Driver del CP
import socket
import sys
import json
import time
import threading
from kafka import KafkaConsumer, KafkaProducer
from queue import Queue
import os

# --- Argumentos de línea de comandos ---
KAFKA_HOST = sys.argv[1]
KAFKA_PORT = int(sys.argv[2])
DRIVER_ID = int(sys.argv[3])

HEADER = 10
FORMAT = 'utf-8'
lock = threading.Lock()


class EstadoDriver:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self.waiting_cp = threading.Event()   
        self.charge_finished = threading.Event() 
        self.charge_in_progress = False
        self.carga_manual = False

estado = EstadoDriver()

# --- Enviar solicitud ---
def request_charge(cp_id):
    with lock:
        estado.waiting_cp.clear()
        estado.charge_finished.clear()
        estado.charge_in_progress = False

    estado.producer.send("driver_requests",{
        "driver_id": DRIVER_ID,
        "cp_id": cp_id,
        "status": "REQUEST_CHARGE"
    })
    estado.producer.flush()
    print(f"[DRIVER {DRIVER_ID}] Solicitud de carga enviada para CP {cp_id}")

    estado.waiting_cp.wait(timeout=10)
    if not estado.waiting_cp.is_set():
        print(f"[DRIVER {DRIVER_ID}] No hubo respuesta de Central. Volviendo al menu.")
        return
    estado.carga_manual = False
    estado.charge_finished.wait()

def mostrar_ticket(cp_id, energia, precio):
    ticket = (
        "\n" +
        "="*40 + "\n" +
        f"         TICKET DE CARGA\n" +
        "="*40 + "\n" +
        f"Punto de carga: CP {cp_id}\n" +
        f"Energía cargada: {energia:.2f} kWh\n" +
        f"Importe total: {precio:.2f} €\n" +
        "="*40 + "\n"
    )
    print(ticket)


def request_cplist():   
    estado.producer.send("driver_requests", {
        "driver_id": DRIVER_ID,
        "status": "LIST_CP"
    })
    estado.producer.flush()
    print(f"[DRIVER {DRIVER_ID}] Solicitando lista de CPs activos...")


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
# --- Inicializar Kafka ---
def hilo_kafka():
    with lock:
        estado.producer = KafkaProducer(
            bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        estado.consumer = KafkaConsumer(
            bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=f"driver_{DRIVER_ID}",
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )
    estado.consumer.subscribe(topics=["driver_commands"])


    while True:
        try:
            kafka_msgs = estado.consumer.poll(timeout_ms=500)
            for messages in kafka_msgs.values():
                for msg in messages:
                    data = msg.value
                    if data.get("driver_id") != DRIVER_ID:
                        continue
                    status = data.get("status")
                    cp_id = data.get("cp_id")
                    with lock:
                        if status == "ACTIVADO":
                            print(f"[DRIVER {DRIVER_ID}] El CP {cp_id} esta activo.")
                            estado.waiting_cp.set()

                        elif status == "CHARGE_FINISHED":
                            print(f"[DRIVER {DRIVER_ID}] CP {cp_id} Carga finalizada")
                            energia = data.get("cp_carga", 0)
                            precio = data.get("price", 0)
                            manual = data.get("manual")
                            mostrar_ticket(cp_id, energia, precio)
                            estado.charge_finished.set()
                            estado.charge_in_progress = False
                            if estado.carga_manual:
                                print(f"[DRIVER {DRIVER_ID}] Carga manual completada. Pulsa ENTER para volver al menú.")
                                

                        elif status == "ESPERANDO":
                            print(f"[DRIVER {DRIVER_ID}] CP {cp_id} esperando a que se enchufe el vehículo")
                            if not estado.waiting_cp.is_set():
                                estado.waiting_cp.set()

                        elif status == "CHARGING":
                            energia = data.get("cp_carga", 0)
                            precio = data.get("price", 0)
                            if not estado.charge_in_progress:
                                print(f"[DRIVER {DRIVER_ID}] Carga iniciada en CP {cp_id}.")
                                estado.charge_in_progress = True
                            print(f"[DRIVER {DRIVER_ID}] Cargando... {energia:.2f} kWh | Importe: {precio:.2f}€")

                        elif status == "CHARGE_ABORTED":
                            print(f"[DRIVER {DRIVER_ID}] El CP {cp_id} ha fallado durante la carga.")
                            energia = data.get("cp_carga", 0)
                            precio = data.get("price", 0)
                            mostrar_ticket(cp_id, energia, precio)
                            estado.waiting_cp.set()
                            estado.charge_finished.set()
                            estado.charge_in_progress = False
                            if estado.carga_manual:
                                print(f"[DRIVER {DRIVER_ID}] Carga manual completada. Pulsa ENTER para volver al menú.")
                                

                        elif status == "CADUCADO":
                            print(f"[DRIVER {DRIVER_ID}] No se ha podido conectar con el CP {cp_id}.")
                            print(f"[DRIVER {DRIVER_ID}] Volviendo al menu principal...")
                            estado.waiting_cp.set()
                            estado.charge_finished.set()
                            estado.charge_in_progress = False

                        elif status == "CP_ACTIVOS":
                            lista = data.get("lista", [])
                            if not lista:
                                print(f"[DRIVER {DRIVER_ID}] No hay CPs activos actualmente.")
                            else:
                                print(f"[DRIVER {DRIVER_ID}] Lista de CPs activos:")
                                for cp in lista:
                                    print(f"   - CP[{cp['cp_id']}] | Localizacion: {cp['localizacion']} | Precio: {float(cp['precio'])}")
                            print()

                        else:
                            print(f"\n[DRIVER {DRIVER_ID}] El CP {cp_id} no disponible, estado {status.lower()}. Elige otro.")             
                            estado.waiting_cp.set()
                            estado.charge_finished.set()
                            estado.charge_in_progress = False



        except Exception as e:
            print(f"[DRIVER {DRIVER_ID}] Error en hilo_kafka: {e}")
            time.sleep(1)




# --- Bucle principal para interactuar con el usuario ---
def bucle_principal():
    clear_screen()
    running = True
    while running: 

            while True:
                estado.carga_manual = True
                try:
                    print("\n--- MENU DRIVER ---")
                    print("1 - Leer archivo de recargas")
                    print("2 - Listar CPs activos")
                    print("3 - Solicitar carga en un CP")
                    print("0 - Salir de la app")
                    opcion = input("Elige una opcion: ").strip().lower()
                    clear_screen()
                    if opcion == "0":
                        running = False
                        break
                   
                    elif opcion == "1":
                        try:
                            with open("recargas_cp.txt", "r") as fich:
                                cp_list = [int(line.strip()) for line in fich if line.strip()]
                            for cp_id in cp_list:
                                request_charge(cp_id)
                                print(f"[DRIVER {DRIVER_ID}] Esperando 4 segundos antes de la siguiente carga...\n")
                                time.sleep(4)
                            print(f"[DRIVER {DRIVER_ID}] Se han procesado todas las cargas del archivo. Volviendo al menu.")
                            continue
                        except FileNotFoundError:
                            print(f"[DRIVER {DRIVER_ID}] Archivo 'recargas_cp.txt' no encontrado.")

                    elif opcion == "2":
                        request_cplist()

                    elif opcion == "3":
                        try:
                            cp_id = int(input("Introduce ID del punto de carga (0 para cancelar): ").strip())
                            if cp_id == 0:
                                continue
                            request_charge(cp_id)
                        except ValueError:
                            print("Introduce un numero valido de CP.")
                    else:
                        if not estado.carga_manual:
                            print("Opcion no valida, elige una de las mostradas.")

                except Exception as e:
                    print(f"[DRIVER {DRIVER_ID}] Error en el menu manual: {e}")
                time.sleep(1)    


# --- MAIN ---
def main():
    threading.Thread(target=hilo_kafka, daemon=True).start()
    bucle_principal()
    estado.producer.close()
    estado.consumer.close()
    print(f"[DRIVER {DRIVER_ID}] Finalizado.")


if __name__ == "__main__":
    main()
