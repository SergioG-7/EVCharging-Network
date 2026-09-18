import ctypes, sys, subprocess, os


CENTRAL_IP = "host.docker.internal" 

def nueva_terminal(cmd, title="EV_W"):
    env = os.environ.copy()
    env["CENTRAL_IP"] = CENTRAL_IP
    
    full_cmd = f'start "EV_W" cmd /k {" ".join(cmd)}'
    # Pasamos env=env al subprocess
    subprocess.run(full_cmd, shell=True, env=env)

# Ejecutamos pasando el entorno
nueva_terminal(["docker", "compose", "run", "--rm", "--service-ports", "ev_w"])