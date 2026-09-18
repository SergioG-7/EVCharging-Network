class ChargingPoint:
    ESTADOS_VALIDOS={"ACTIVADO", "PARADO", "SUMINISTRANDO", "AVERIADO", "DESCONECTADO"}
    # Constructor por defecto.
    def __init__(self, id, localizacion, precio, estado="DESCONECTADO",token=None):
        self.id=id
        self.localizacion=localizacion
        self.precio=precio
        if estado in self.ESTADOS_VALIDOS:
            self.estado=estado
        self.ping=0.0
        self.clave_simetrica=None
        self.token=token
        self.socket = None

    def __str__(self):
        return f"CP[{self.id}] | Localizacion: {self.localizacion} | Precio: {self.precio} /kwh | Estado: {self.estado}"