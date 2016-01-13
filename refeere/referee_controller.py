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




###############################################################################
## PROCEDURES                                                                ##
###############################################################################
##
## BRIEF: Cadastra um novo player.
## ----------------------------------------------------------------------------
## @PARM playerName     == nome do player.
## @PARM playerEndpoint == endereco do player.
##
def add_new_player(dictPlayer): 
    valRets = "";

    ## Conecta ao referee_core (pode ser implementado outra classe Authentica-
    ## tion) via socket. Mudar no futuro para AMQP.
    try:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        connection.connect(("127.0.0.1", 8000));
    except:
        return -1;

    ## Messagem 002 refere-se a verificacao de token.
    message  = "001|";
    message += str(dictPlayer["name"])         + ",";
    message += str(dictPlayer["endpoint"])     + ",";
    message += str(dictPlayer["os"])           + ",";
    message += str(dictPlayer["plataform"])    + ",";
    message += str(dictPlayer["hypervisor"])   + ",";
    message += str(dictPlayer["country"])      + ",";
    message += str(dictPlayer["division"]);

    connection.sendall(message);

    ## Obtem o valor de retorno da validacao d token.
    valRets = connection.recv(1024);

    ## Encerra a conexao apos realizar o procedimento de consulta e validacao
    ## do token.
    connection.close(); 

    return valRets;
## End.



##
## BRIEF: Descadastra um novo player.
## ----------------------------------------------------------------------------
## @PARM playerToken == token do player que quer se descadastrar.
##
def del_new_player(dictPlayer):
    valRets = "";

    ## Conecta ao referee_core (pode ser implementado outra classe Authentica-
    ## tion) via socket. Mudar no futuro para AMQP.
    try:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        connection.connect(("127.0.0.1", 8000));
    except:
        return -1;

    ## Messagem 003 refere-se a verificacao de token.
    message  = "002|";
    message += str(dictPlayer["token"]);

    connection.sendall(message);

    ## Obtem o valor de retorno da validacao d token.
    valRets = connection.recv(1024);

    ## Encerra a conexao apos realizar o procedimento de consulta e validacao
    ## do token.
    connection.close();

    return valRets;
## End.



##
## BRIEF: Verifica se eh um token valido.
## ----------------------------------------------------------------------------
## @PARM playerToken == token do player para validar.
##
#def check_token(playerToken): 
    ## Conecta ao referee_core (pode ser implementado outra classe Authentica-
    ## tion) via socket. Mudar no futuro para AMQP.
#    try:
#        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
#        connection.connect(("127.0.0.1", 8000));
#    except:
#        return -1;

    ## Messagem 002 refere-se a verificacao de token.
#    message = "003|"+str(playerToken);

#    connection.sendall(message);
  
    ## Obtem o valor de retorno da validacao d token.
#    valRets = connection.recv(1024);

    ## Encerra a conexao apos realizar o procedimento de consulta e validacao
    ## do token.
#    connection.close(); 

#    if valRets == "1":
#        return 1;
#    else:
#        return 0;
## End.


##
## BRIEF: Envia a requisicao para o referee core.
## ----------------------------------------------------------------------------
## @PARM playerToken == token do player para validar.
## @PARM vmType      == tipode da vm para ser criada.
##
def send_request(playerToken, vmType): 
    ## Conecta ao referee_core (pode ser implementado outra classe Authentica-
    ## tion) via socket. Mudar no futuro para AMQP.
    try:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        connection.connect(("127.0.0.1", 8000));
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
        return 'Refeere version 2';

    def POST(self, **kwargs):
        return 'Refeere version 2';
## End.







class Subscribe(Controller):
    """
    ---------------------------------------------------------------------------
    """

    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    ##
    ## BRIEF: Esse metodo nao esta disponivel.
    ## ------------------------------------------------------------------------
    ##
    def GET(self):
        return "Method Unavaliable";


    ##
    ## BRIEF: Cada novo player realiza um POST para receber o token de autenti-
    ##        cacao q/ permite fazer parte do TMN.
    ## ------------------------------------------------------------------------
    ##
    def POST(self, **kwargs):
        auth_token = "";
        ##
        auth_token = add_new_player(kwargs);
        return auth_token;
## End.








class Unsubscribe(Controller):
    """
    ---------------------------------------------------------------------------
    """

    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    ##
    ## BRIEF: Esse metodo nao esta disponivel.
    ## ------------------------------------------------------------------------
    ##
    def GET(self):
        return "Method Unavaliable";


    ##
    ## BRIEF: Cada novo player realiza um POST para receber o token de autenti-
    ##        cacao q/ permite fazer parte do TMN.
    ## ------------------------------------------------------------------------
    ##
    def POST(self, **kwargs):
        ##
        valRet = del_new_player(kwargs);
        return valRet;
## End.








class Vm(Controller):
    """
    ---------------------------------------------------------------------------
    Codigos de retorno HTTP:
    400 == Bad Request. Verificar se esse retorno eh o mais indicado!
    401 == Unauthorized.
    201 == Created.
    405 == Method Not Allowed.
    """

    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    ##
    ## BRIEF: Esse metodo nao esta disponivel.
    ## ------------------------------------------------------------------------
    ##
    def GET(self):
        return 405;


    ##
    ## BRIEF: Recebe a requisicao da instanciacao de uma VM.
    ## ------------------------------------------------------------------------
    ##
    def POST(self, **kwargs):
        valRet = 'reject';

        ## ---------------------------- ## 
        ##  1 == vm created;            ##
        ##  0 == vm creation reject;    ##
        ## -1 == arguments invalid;     ##
        ## -2 == vm type not supported; ##
        ## -3 == token invalid;         ##
        ## ---------------------------- ## 
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


