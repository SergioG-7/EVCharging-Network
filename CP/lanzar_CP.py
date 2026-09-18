import subprocess, os, time, sys

def nueva_terminal(cmd, title=None):
    full_cmd = f'start {"cmd /k " if title is None else f'"{title}" cmd /k '}{" ".join(cmd)}'
    subprocess.run(full_cmd, shell=True)

CPS = {
    "1": ("engine1", "monitor1"),
    "2": ("engine2", "monitor2"),
    "3": ("engine3", "monitor3"),
    "4": ("engine4", "monitor4"),
    "5": ("engine5", "monitor5"),
    "6": ("engine6", "monitor6"),
    "7": ("engine7", "monitor7"),
    "8": ("engine8", "monitor8"),
    "9": ("engine9", "monitor9"),
    "10": ("engine10", "monitor10")
}

KAFKA_IP = "host.docker.internal"
CENTRAL_IP = "host.docker.internal"
REGISTRY_IP = "host.docker.internal"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python lanzar_CP.py <id> [engine|monitor]")
        print("Ejemplos:")
        print("  python lanzar_CP.py 1         # engine + monitor")
        print("  python lanzar_CP.py 1 engine  # solo engine1")
        print("  python lanzar_CP.py 1 monitor # solo monitor1")
        sys.exit(1)

    cp_id = sys.argv[1]
    componente = sys.argv[2].lower() if len(sys.argv) > 2 else None
    if cp_id not in CPS:
        print(f"CP {cp_id} no existe en el compose.")
        sys.exit(1)

    engine_name, monitor_name = CPS[cp_id]


    if componente == "engine":
        cps_levantar = [engine_name]
    elif componente == "monitor":
        cps_levantar = [monitor_name]
    else:
        cps_levantar = [engine_name, monitor_name]
   
    env = os.environ.copy()
    env["KAFKA_IP"] = KAFKA_IP
    env["CENTRAL_IP"] = CENTRAL_IP
    env["REGISTRY_IP"] = REGISTRY_IP
    

    for nombre in cps_levantar:
        print(f"\nLevantando {nombre} ...")
        subprocess.run(["docker", "compose", "up", "-d","--no-deps", "--build", nombre], check=True, env=env)
    
    print("Esperando a que los contenedores estén listos...")
    time.sleep(1)
    
    # --- Abrir terminales y logs solo para los levantados ---
    if engine_name in cps_levantar:
        print(f"Abriendo terminal para {engine_name}...")
        nueva_terminal(["docker", "attach", engine_name], title=engine_name)
        time.sleep(0.5)

        # Logs del engine
        workdir = os.getcwd().replace("'", "''")
        ps_cmd = (
            f"$host.UI.RawUI.WindowTitle='{engine_name}.log'; "
            f"Set-Location -LiteralPath '{workdir}'; "
            f"while (-not ($cid = docker ps -q -f \"name={engine_name}\")) {{ Start-Sleep -Seconds 1 }}; "
            f"docker exec -i $cid sh -c 'tail -n 200 -f /app/engine.log'"
        )
        nueva_terminal(["powershell", "-NoExit", "-NoProfile", "-Command", ps_cmd], title=f"{engine_name}.log")


    if monitor_name in cps_levantar:
        print(f"Abriendo terminal para {monitor_name}...")
        nueva_terminal(["docker", "attach", monitor_name], title=monitor_name)
        time.sleep(0.5)

        # Logs del monitor
        workdir = os.getcwd().replace("'", "''")
        ps_cmd = (
            f"$host.UI.RawUI.WindowTitle='{monitor_name}.log'; "
            f"Set-Location -LiteralPath '{workdir}'; "
            f"while (-not ($cid = docker ps -q -f \"name={monitor_name}\")) {{ Start-Sleep -Seconds 1 }}; "
            f"docker exec -i $cid sh -c 'tail -n 200 -f /app/monitor.log'"
        )
        nueva_terminal(["powershell", "-NoExit", "-NoProfile", "-Command", ps_cmd], title=f"{monitor_name}.log")

    print("Contenedores levantados y terminales abiertas correctamente")