import subprocess, sys, ctypes

def nombre_consola(nombre):
    if sys.platform == "win32":
        ctypes.windll.kernel32.SetConsoleTitleW(nombre)

if __name__ == "__main__":
    print("INICIANDO EV_REGISTRY (HTTPS)...")
    nombre_consola("EV_REGISTRY")
    subprocess.run(["docker", "compose", "up", "--build", "registry"], check=True)
