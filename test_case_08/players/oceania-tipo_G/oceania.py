#!/usr/bin/python


###############################################################################
## IMPORTS                                                                   ##
###############################################################################
import os;
import requests;
import json;
import ConfigParser;
import time;
import socket;
import logging;

from subprocess                 import call;
from endpoints.interface.simple import Server;
from multiprocessing            import Process, Queue, Lock, Array;








###############################################################################
## DEFINITIONS                                                               ##
###############################################################################
INTERNAL_PORT = 7004;





###############################################################################
## CLASSES                                                                   ##
###############################################################################
class Vm(Process):

    """
    ---------------------------------------------------------------------------
    """
    ###########################################################################
    ## ATTRIBUTES                                                            ## 
    ###########################################################################
    __type = None;
    __lock = None;
    __array= None;


    ###########################################################################
    ## SPECIAL METHODS                                                       ## 
    ###########################################################################
    def __init__(self, array, vmType):
        super(Vm, self).__init__();

        self.__type = vmType;
        self.__array= array;


    ###########################################################################
    ## PUBLIC METHODS                                                        ## 
    ###########################################################################
    ##
    ## BRIEF:
    ## ------------------------------------------------------------------------
    ##
    def run(self):

        if   self.__type == 'T':
            time.sleep(30);
            self.__array[0] += 1; 

        elif self.__type == 'S':
            time.sleep(30);
            self.__array[1] += 1; 

        elif self.__type == 'B':
            time.sleep(30);
            self.__array[2] += 1; 

        return 1;
## End.




class PlayerSnd(Process):

    """
    ---------------------------------------------------------------------------
    """
    ###########################################################################
    ## ATTRIBUTES                                                            ## 
    ###########################################################################
    __authToken  = None;
    __vmTypes    = None;


    ###########################################################################
    ## SPECIAL METHODS                                                       ## 
    ###########################################################################
    def __init__(self, lock, authToken, vmTypes):
        super(PlayerSnd, self).__init__();

        print 'Instanciando o componente de envio de requisicao';

        self.__authToken = authToken;
        self.__vmTypes   = vmTypes;


    ###########################################################################
    ## PUBLIC METHODS                                                        ## 
    ###########################################################################
    def run(self):
        print 'Executando o componente de envio de requisicao';

        h = {'Content-type': 'application/json'};
        u = 'http://localhost:50000/Vm';

        while True:
            if self.__vmTypes['T'] == 'send':
                print 'Enviando requisicao para instancia de VM T';
                d =  {'auth_token'  : self.__authToken, 'type': 'T'};
                try:
                    r = requests.post(u, data=json.dumps(d), headers=h);
                    print 'Requisicao: ' + r.text;
                except:
                    pass;

            if self.__vmTypes['S'] == 'send':
                print 'Enviando requisicao para instancia de VM S';
                d =  {'auth_token'  : self.__authToken, 'type': 'S'};
                try:
                    r = requests.post(u, data=json.dumps(d), headers=h);
                    print 'Requisicao: ' + r.text;
                except:
                    pass;

            if self.__vmTypes['B'] == 'send':
                print 'Enviando requisicao para instancia de VM B';
                d =  {'auth_token'  : self.__authToken, 'type': 'B'};
                try:
                    r = requests.post(u, data=json.dumps(d), headers=h);
                    print 'Requisicao: ' + r.text;
                except:
                    pass;

            time.sleep(4);
## End.




class PlayerRcv(Process):

    """
    ---------------------------------------------------------------------------
    """
    ###########################################################################
    ## ATTRIBUTES                                                            ## 
    ###########################################################################
    __lock       = None;
    __name       = None;
    __endpoint   = None;
    __os         = None;
    __plataform  = None;
    __hypervisor = None;
    __country    = None;
    __vmsTypeT   = None;
    __vmsTypeS   = None;
    __vmsTypeB   = None;
    __authToken  = None;
    __array      = None;


    ###########################################################################
    ## SPECIAL METHODS                                                       ## 
    ###########################################################################
    def __init__(self, lock, array, authToken, dictData):
        super(PlayerRcv, self).__init__();
    
        print 'Instanciando o componente de recebimento de requisicao';

        self.__name       = dictData['name'      ];
        self.__endpoint   = dictData['endpoint'  ];
        self.__os         = dictData['os'        ];
        self.__plataform  = dictData['plataform' ];
        self.__hypervisor = dictData['hypervisor'];
        self.__country    = dictData['country'   ];

        self.__authToken  = authToken;
        self.__lock       = lock;
        self.__array      = array;


    ###########################################################################
    ## PUBLIC METHODS                                                        ## 
    ###########################################################################
    def run(self):

        print 'Executando o componente de recebimento de requisicao';

        valRet = 1;

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1);

        s.bind(("127.0.0.1", INTERNAL_PORT));
        s.listen(5);

        while True:
           ## Aguarda por novas conexoes e novos dados para executar as tarefas
           ## apropriadas;
           connection, address = s.accept();

           message = connection.recv(1024);

           if   message == "000":
               break;
           else:
               codOper = message.split("|")[0];
               payload = message.split("|")[1];

               ## Codigo 003 refere-se ao pedido de instanciacao de uma nova VM
               if codOper == "003":
                   print 'Requisicao para criacao de uma VM: ' + str(payload);
                   valRet = self.__new_virtual(payload)

               ## Verifica o tipo de VM.
               connection.sendall(valRet);
               connection.close();

        connection.close();
        return 1;


    ##
    ## BRIEF: Instancia, se possivel, uma nova maquina virtual.
    ## ------------------------------------------------------------------------
    ## @PARAM payload == os dados do player. 
    ##
    def __new_virtual(self, payload):
        valRet = 'reject';

        ## Auth_token e tipo de maquina virtual.
        token  = payload.split(",")[0];
        typeVm = payload.split(",")[1];

        print 'Quantidade de vms: ' + str(self.__array[0]);

        if   typeVm == 'T' and self.__array[0] > 0:
            self.__array[0] -= 1;
            print 'Disponiveis ' + str(self.__array[0]);

            vm = Vm(self.__array, 'T');
            vm.start();

            valRet = 'accept';

        elif typeVm == 'S' and self.__array[1] > 0:
            self.__array[1] -= 1;
            print 'Disponiveis ' + str(self.__array[1]);

            vm = Vm(self.__array, 'S');
            vm.start();

            valRet = 'accept';

        elif typeVm == 'B' and self.__array[2] > 0:
            self.__array[2] -= 1;
            print 'Disponiveis ' + str(self.__array[2]);

            vm = Vm(self.__array, 'B');
            vm.start();

            valRet = 'accept';

        ## O valor de retorno significa 0 para requisicao nao atendida e 1 para
        ## requisiao atendida.
        return valRet;

## End.








###############################################################################
## MAIN                                                                      ##
###############################################################################
if __name__ == "__main__":
    ## Executa o config parse para recuperar os parametros referente a execucao
    ## das divisoes.
    config = ConfigParser.RawConfigParser();
    config.read('config.ini');

    name       = config.get('main', 'name');
    endpoint   = config.get('main', 'endpoint');
    osystem    = config.get('main', 'os');
    plataform  = config.get('main', 'plataform');
    hypervisor = config.get('main', 'hypervisor');
    country    = config.get('main', 'country');
    controller = config.get('main', 'controller');
    division   = config.get('main', 'division');

    dictData =  {
        "name"      : name, 
        "endpoint"  : endpoint,
        "os"        : osystem,
        "plataform" : plataform,
        "hypervisor": hypervisor,
        "country"   : country,
        "division"  : division
    };

    ##
    array = Array('i', range(3));

    array[0] = config.getint('virtual machines', 'tinny');
    array[1] = config.getint('virtual machines', 'small');
    array[2] = config.getint('virtual machines', 'big'  );

    ## Antes de iniciar o server, eh realizada uma autenticacao no refeere para
    ## obter o token de autenticacao.
    headers  =  {'Content-type': 'application/json'};
    url      =  "http://localhost:50000/Subscribe";
    r = requests.post(url, data=json.dumps(dictData), headers=headers);
    authToken = r.text.replace('"','');

    ## Esse lock eh utilizado p/ manter a sincronizacao de escrita no database.
    lock = Lock();

    ## Coloca o player em estado de execucao, enviando requisicoes de vms e re-
    ## cebendo requisicoes.
    playerRcv = PlayerRcv(lock, array, authToken, dictData);
    playerRcv.start();

    playerSnd = PlayerSnd(lock, authToken, {'T':'send', 'S':'send', 'B':'send'});
    playerSnd.start();

    ##
    os.environ['ENDPOINTS_PREFIX']     = controller;
    os.environ['ENDPOINTS_SIMPLE_HOST']= endpoint.replace('http://','');

    try:
       player = Server();
       player.serve_forever();

    ## Caso Ctrl-C seja precionado realiza os procedimentos p/ finalizar o ambi
    ## ente.
    except KeyboardInterrupt, e:
        pass;

    playerSnd.terminate();
    playerRcv.terminate();

## End.
