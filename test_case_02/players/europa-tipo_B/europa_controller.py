#!/usr/bin/python




###############################################################################
## IMPORT                                                                    ##
###############################################################################
import os;
import socket;

from endpoints import Controller




###############################################################################
## DEFINITION                                                                ##
###############################################################################
SUPORTED_VM_TYPE = ['T', 'S', 'B'];
INTERNAL_PORT    = 7003;



###############################################################################
## PROCEDURES                                                                ##
###############################################################################
##
## BRIEF: Envia a requisicao para o player.
## ----------------------------------------------------------------------------
## @PARM playerToken == token do player para validar.
## @PARM vmType      == tipode da vm para ser criada.
##
def send_request(playerToken, vmType):
    ## Conecta ao referee_core (pode ser implementado outra classe Authentica-
    ## tion) via socket. Mudar no futuro para AMQP.
    try:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        connection.connect(("127.0.0.1", INTERNAL_PORT));
    except:
        return 'reject';

    ## Messagem 003 refere-se a criacao de nova VM.
    message = "003|"+str(playerToken)+","+str(vmType);

    connection.sendall(message);

    ## Obtem o valor de retorno da requisicao de criacao da maquina virtual VM.
    valRet = connection.recv(1024);
    connection.close();

    return valRet;
## End.




###############################################################################
## CLASSES                                                                   ##
###############################################################################
class Default(Controller):
    """
    ---------------------------------------------------------------------------
    """

    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    def GET(self):
        return 'layer 1';

    def POST(self, **kwargs):
        return 'Player 1';
## End.




class Properties(Controller):
    """
    ---------------------------------------------------------------------------
    """

    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    def GET(self):
        buffer  = "";
        buffer += "os:Linux;";
        buffer += "plataform:x86;";
        buffer += "hypervisor:Xen;";
        buffer += "timezone:10;";
        buffer += "country:Brasil;";

        return buffer;
## End.




class Vm(Controller):
    """
    ---------------------------------------------------------------------------
    """

    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    def GET(self):
        return "Player 1"


    ##
    ## BRIEF: Recebe a requisicao da instanciacao de uma VM.
    ## ------------------------------------------------------------------------
    ##
    def POST(self, **kwargs):
        valRet = "reject";

        try:
            playerToken = kwargs["auth_token"];
            vmType      = kwargs["type" ];
        except:
            return valRet;

        ## Verifica o tipo se eh valido {T S,B}. Caso nao seja valido aborta o
        ## procedimento.
        if any(vmType in s for s in SUPORTED_VM_TYPE):
            valRet = send_request(playerToken, vmType);

        return valRet;
## End.
