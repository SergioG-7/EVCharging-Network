import subprocess,os,sys,ctypes
from ctypes import wintypes


def nombre_consola(nombre):
    if sys.platform=="win32":
        ctypes.windll.kernel32.SetConsoleTitleW(nombre)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python lanzar_Driver.py <id>       -> Para iniciar un Driver con ese ID")
        sys.exit(1)

    DRIVER_ID = sys.argv[1]
    KAFKA_IP = "host.docker.internal"
    
    env = os.environ.copy()
    env["KAFKA_IP"] = KAFKA_IP
    env["DRIVER_ID"] = DRIVER_ID
    nombre_consola(f"Driver {DRIVER_ID}")
    subprocess.run(["docker", "compose", "run","--build","--rm","--service-ports", "driver"], check=True, env=env)

