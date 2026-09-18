import ctypes, sys, subprocess, os
from ctypes import wintypes

def nueva_terminal(cmd, nombre="Central.log"):
    full_cmd=f'start {"cmd /k " if nombre is None else f'"{nombre}" cmd /k '}{" ".join(cmd)}'
    subprocess.run(full_cmd, shell=True)

# Funcion para cambiar el nombre de la ventana de consola actual.
def nombre_consola(nombre):
    if sys.platform=="win32":
        ctypes.windll.kernel32.SetConsoleTitleW(nombre)

# Funcion que desactiva el QuickEdit de Windows para evitar errores en la terminal.
def desactivar_quickedit():
    if sys.platform!="win32":
        return False
    STD_INPUT_HANDLE=-10
    ENABLE_QUICK_EDIT_MODE=0x0040
    ENABLE_INSERT_MODE=0x0020
    ENABLE_EXTENDED_FLAGS=0x0080
    k32=ctypes.WinDLL('kernel32', use_last_error=True)
    h=k32.GetStdHandle(STD_INPUT_HANDLE)
    if not h or h==ctypes.c_void_p(-1).value:
        return False
    mode=wintypes.DWORD()
    if not k32.GetConsoleMode(h, ctypes.byref(mode)):
        return False
    mode.value|=ENABLE_EXTENDED_FLAGS
    mode.value&=~(ENABLE_QUICK_EDIT_MODE | ENABLE_INSERT_MODE)
    ok=k32.SetConsoleMode(h, mode)
    return bool(ok)

# Funcion para ejecutar comandos en la terminal.
def run(cmd):
    print("$ "+" ".join(cmd))
    r=subprocess.run(cmd)
    if r.returncode!=0:
        sys.exit(r.returncode)

if desactivar_quickedit():
    print("QuickEdit desactivado para esta consola.")
else:
    print("QuickEdit no se ha podido desactivar para esta consola.")
# Comandos.
run(["docker", "compose", "up", "-d", "--build", "broker", "db", "api_central","front"])
run(["powershell", "-NoProfile", "-Command", "Start-Process 'http://localhost:8081/'"])
workdir=os.getcwd().replace("'", "''")
ps_cmd=(
    f"$host.UI.RawUI.WindowTitle='Central.log'; "
    f"Set-Location -LiteralPath '{workdir}'; "
    "while (-not ($cid = docker ps -q -f \"label=com.docker.compose.service=central\")) { Start-Sleep -Seconds 1 }; "  
    "docker exec -i $cid sh -c 'tail -n 200 -f /app/central.log'"
)
nueva_terminal(["powershell", "-NoExit", "-NoProfile", "-Command", ps_cmd], "Central.log")
nombre_consola("Central")
run(["docker", "compose", "run", "--build", "--rm", "--service-ports", "central"])