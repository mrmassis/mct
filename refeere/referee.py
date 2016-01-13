#!/usr/bin/python


###############################################################################
## IMPORTS                                                                   ##
###############################################################################
import os;
import sys;
import threading;
import time;
import socket;
import hashlib;
import requests;
import json;
import mysql;
import mysql.connector;
import traceback;
import datetime;
import ConfigParser;
import logging;

from subprocess                 import call;
from endpoints.interface.simple import Server;
from multiprocessing            import Process, Queue, Lock;

from lib.config                 import Config;




###############################################################################
## DEFINITIONS                                                               ##
###############################################################################
INTERNAL_PORT = 8000;




###############################################################################
## CLASSES                                                                   ##
###############################################################################
class Database:

    def __init__(self, dhost, duser, dpass, dname):
        self.__dbConnection = self.__db_connect(dhost, duser, dpass, dname);


    ##
    ## DESCRICAO: realiza a conexao com o banco de dados.
    ## ------------------------------------------------------------------------
    ## @PARAM dbHost == host onde esta o banco de dados.
    ## @PARAM dbUser == usuario do banco com permissao de acesso.
    ## @PARAM dbPass == senha do usuario.
    ## @PARAM dbName == nome da base de dados de interesse..
    ##
    def __db_connect(self, dbHost, dbUser, dbPass, dbName):

        dbData = {
           'user':             dbUser,
           'password':         dbPass,
           'host':             dbHost,
           'database':         dbName,
           'raise_on_warnings': True,
        }

        ## Realiza a conexao com o banco a partir das informacoes definidas no
        ## dicionario dbData.
        try:
            connection = mysql.connector.connect(**dbData);

        ## Caso por algum motivo adverso nao foi possivel conecta a base de da
        ## dos realiza o tratamento de erros.
        except mysql.connector.Error as err:

            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password");
                return None;

            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist");
                return None;

            else:
                print(err);
                return None;

        return connection;


    ##
    ## BRIEF:
    ## -----------------------------------------------------------------------
    ## 
    def insert_query(self, query, value):

        try:
            cursor = self.__dbConnection.cursor();
            cursor.execute(query, value);
            self.__dbConnection.commit();

            cursor.close();

        except mysql.connector.Error as err:
            print(err);

        return 1;


    ##
    ## BRIEF:
    ## -----------------------------------------------------------------------
    ## 
    def select_query(self, query):
        entry = [];

        try:
            cursor = self.__dbConnection.cursor();
            cursor.execute(query);
            for row in cursor:
                entry.append(row);

            cursor.close();

        except mysql.connector.Error as err:
            print(err);

        return entry;


    ##
    ## BRIEF:
    ## -----------------------------------------------------------------------
    ## 
    def delete_query(self, query):
        try:
            cursor = self.__dbConnection.cursor();

            cursor.execute(query);
            self.__dbConnection.commit();
            cursor.close();

        except mysql.connector.Error as err:
            print(err);

        return 1;


    ##
    ## BRIEF:
    ## -----------------------------------------------------------------------
    ## 
    def update_query(self, query, value=()):
        try:
            cursor = self.__dbConnection.cursor();
            cursor.execute(query, value);
            self.__dbConnection.commit();

            cursor.close();

        except mysql.connector.Error as err:
            print(err);

        return 1;
## End.




class Attributes:
    """
    Calcula os atributos dos jogadores do torneio.
    ---------------------------------------------------------------------------
    """
    ###########################################################################
    ## ATTRIBUTES                                                            ##
    ###########################################################################
    __mysql    = None;
    __lock     = None;
    __division = None;
    __sMin     = None;
    __sMax     = None;


    ###########################################################################
    ## SPECIAL METHODS                                                       ##
    ###########################################################################
    def __init__(self, lock, mysql, division, sMin, sMax):
        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'Instanciando o Attributes';
        logging.info(buffer);

        self.__mysql    = mysql;
        self.__lock     = lock;
        self.__division = division;
        self.__sMin     = sMin;
        self.__sMax     = sMax;


    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    def calculate(self):
        buffer  ='[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer +='Calculando os atribs p players da div: '+str(self.__division);
        logging.info(buffer);

        ## Seleciona todos os players que esta presentes n divisao de interesse.
        query="SELECT * FROM STATUS WHERE division='"+str(self.__division)+"'";

        self.__lock.acquire();
        valRet = self.__mysql.select_query(query);    
        self.__lock.release();


        for player in valRet:
            newScore = self.__calc_score(player);

            ## Insere uma nova entrada na tabela de HISTORICO com o score cal-
            ## culado. O valor eh utilizado para situacoes quando o player eh
            ## enviado a playoff.
            timeStamp = str(datetime.datetime.now());

            query = ("INSERT INTO HISTORIC (timestamp, token, score, valid) "
                     "VALUES (%s, %s, %s, %s)");
            value = (timeStamp, player[0], player[7], 1);

            self.__lock.acquire();
            self.__mysql.insert_query(query, value);    
            self.__lock.release();

            ## Apaga o numero de vms aceitas e rejeitadas e cada player p/ au-
            ## xiliar nos proximos rounds.
            self.__set_attributes(player, newScore);

    
    ##
    ## BRIEF: realiza o calculo dos atributos iniciais para os novos players.
    ## ------------------------------------------------------------------------
    ##
    def initial_atributes(self):
        scores = [];

        ## Realiza o calculo do score inicial. Seleciona todos os player da di-
        ## visao. Apos realiza a media harmonica dos scores obtidos.
        query  = "SELECT score FROM STATUS WHERE division=3";

        self.__lock.acquire();
        valRet = self.__mysql.select_query(query);
        self.__lock.release();

        i = 0;
        j = 0.0;
              
        for (score) in valRet:
            if score[0] == 0.0:
               continue;
            else:
               i += 1;
               j += (1.0/score[0]);

        ## Evita erros de divisao por zero que possam ocorrer no calculo da me-
        ## dia harmonica.
        try:
            resultScore = i/j;
        except:
            resultScore = 0.1;

        return resultScore;


    ##
    ## BRIEF: realiza a atualizacao dos dados referente a aceitar ou nao VMs.
    ## ------------------------------------------------------------------------
    ## @PARAM token  == token de identificacao do player.
    ## @PARAM valRet == valor de retorno do player.
    ## @PARAM vType  == tipo da VM.
    ##
    def update(self, token, valRet, vType):
        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'Atualizando os atributos do player ' + token;
        logging.info(buffer);

        ## Contabiliza na tabela de status do player a vm aceita ou rejeitada. 
        if valRet == '"accept"':
           field = "vm_" + vType.lower() + "_accept";
        else:
           field = "vm_" + vType.lower() + "_reject";

        query=("UPDATE STATUS SET " + field + " = " + field + " + 1 "
               "WHERE token = '" + token + "'");

        self.__lock.acquire();
        self.__mysql.update_query(query);    
        self.__lock.release();

        return 1;


    ###########################################################################
    ## PRIVATE METHODS                                                       ##
    ###########################################################################
    ##
    ## BRIEF: calcula o score de cada player.
    ## ------------------------------------------------------------------------
    ## @PARAM player == player de interesse.
    ##
    def __calc_score(self, player):
        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'Calculando o score do player ' + player[0];
        logging.info(buffer);

        ## TODO: verificar a taxa de producao e consumo! Pode acontecer que de-
        ##       vido a taxa pode ser gerado um score negativo mesmo tendo um
        ##       bom consumo.

        ## TODO: score por divisao, considerando somente as VMs da divisao. Ve-
        ##       rificar isso.

        ## ----------------------------------------------------------------- ##
        ##  SUM(k=b,s,t (p_k x I_vm_k)) - qtde_reject x cost_reject          ##
        ## ----------------------------------------------------------------- ##
        if   self.__division == 3:
            vmsAccepts = player[1];
            vmsRejects = player[4];
            weight     = 1;

        elif self.__division == 2:
            vmsAccepts = player[2];
            vmsRejects = player[5];
            weight     = 2;

        else:
            vmsAccepts = player[3];
            vmsRejects = player[6];
            weight     = 4;

        ## O custo eh fixo. Definir bem o custo: 
        part_1 = (vmsAccepts * weight);
        part_2 = (vmsRejects * 0.1);

        score = float(part_1 - part_2 + 0.1);

        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'O novo score do player ' + player[0] + ' eh ' + str(score);
        logging.info(buffer);

        return score;

    
    ##
    ## BRIEF: ajusta os atributos do player no banco.
    ## ------------------------------------------------------------------------
    ## @PARAM player   == player de interesse.
    ## @PARAM newScore == novo score do player.
    ##
    def __set_attributes(self, player, newScore):
        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'Calculando o score do player ' + player[0];
        logging.info(buffer);

        division = player[8];
        playoffs = player[9];

        vmAccept = '';
        vmReject = '';

        print player


        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'Atributos do player antes do ajuste: "' + player[0] + '": ';
        buffer += 'divisao..: ' + str(division) + ' ';
        buffer += 'playoffs.: ' + str(playoffs) + ' ';
        buffer += 'novoScore: ' + str(newScore);
        logging.info(buffer);

        ## ----------------------------------------------------------------- ##
        ## CASO 1: Score dentro da faixa minima e maxima da divisao atual.   ##
        ## ----------------------------------------------------------------- ##
        if newScore >= self.__sMin and newScore < self.__sMax:
            playoffs = 0;

        ## ----------------------------------------------------------------- ##
        ## CASO 2: Score menor que o minimo permitido p/ a divisao atual.    ##
        ## ----------------------------------------------------------------- ##
        elif newScore < self.__sMin:

            ## Caso o player analisado esteja nos playoffs verifica a quantida-
            ## de de rounds que ele esta nessa situacao. Cc vai p/ os playoffs.
            if playoffs > 0:
                playoffs -= 1;

                if   playoffs == 0 and division == 3:
                    self.__eliminate_player(player);

                elif playoffs == 0 and division != 3:
                    vmAccept = "vm_t_accept=0, vm_s_accept=0, vm_b_accept=0, ";
                    vmReject = "vm_t_reject=0, vm_s_reject=0, vm_b_reject=0, ";

                    newScore  = 0.1;
                    division += 1;

            else:
                ## Vai para os playoffs por 'n' rounds, n depende do historico.
                playoffs = self.__historic(player);


        ## ----------------------------------------------------------------- ##
        ## CASO 3: Score maior que o maximo da divisao atual.                ##
        ## ----------------------------------------------------------------- ##
        elif newScore >= self.__sMax:

            ##
            query ="SELECT max_division FROM PLAYER WHERE token='"+player[0]+"'";

            self.__lock.acquire();
            valRet = self.__mysql.select_query(query);
            self.__lock.release();
           
            ## Verifica se o player quer mudar de divisao ou prefere se estabe-
            ## lecer na mesma.
            if int((valRet[0])[0]) != self.__division:
                vmAccept = "vm_t_accept=0, vm_s_accept=0, vm_b_accept=0, ";
                vmReject = "vm_t_reject=0, vm_s_reject=0, vm_b_reject=0, ";

                newScore  = 0.1;
                division -= 1;

            playoffs  = 0;
            
        ## Realiza atualizacao da base de dados com as informacoes pertinentes.
        request  = "UPDATE STATUS SET "
        request += vmAccept;
        request += vmReject;
        request += "division=" + str(division) + ", " ;
        request += "playoff="  + str(playoffs) + ", " ; 
        request += "score="    + str(newScore) + "  " ;
        request += "WHERE token = '" + player[0] + "'";

        query=(request);

        self.__lock.acquire();
        self.__mysql.update_query(query);    
        self.__lock.release();

        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'Atributos do player depois do ajuste "' + player[0] + '": ';
        buffer += 'divisao..: ' + str(division) + ' ';
        buffer += 'playoffs.: ' + str(playoffs) + ' ';
        buffer += 'novoScore: ' + str(newScore);
        logging.info(buffer);

        return 1;
    

    ##
    ## BRIEF: elimina um player do torneio.
    ## ------------------------------------------------------------------------
    ## @PARAM player == player de interesse.
    ##
    def __eliminate_player(self, player):
        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'Eliminando o player ' + player[0];
        logging.info(buffer);

        query = "DELETE FROM PLAYER   WHERE token='" + player[0] + "'";
        self.__lock.acquire();
        self.__mysql.delete_query(query);    
        self.__lock.release();

        query = "DELETE FROM STATUS   WHERE token='" + player[0] + "'";
        self.__lock.acquire();
        self.__mysql.delete_query(query);    
        self.__lock.release();

        return 0;


    ##
    ## BRIEF: Calcula o historico do player para verificar quanto tempo ira fi-
    ##        car no playoff.
    ## ------------------------------------------------------------------------
    ## @PARAM player == player de interesse.
    ##
    def __historic(self, player):
        buffer  = '[ATTRIBUTES DIVISION ' + str(self.__division) + '] ';
        buffer += 'Calculando o historico e gerando os rounds de ' + player[0];
        logging.info(buffer);

        query   = "SELECT * FROM HISTORIC WHERE " ;
        query  += "token='" + player[0] + "' and ";
        query  += "valid=1";

        self.__lock.acquire();
        valRet = self.__mysql.select_query(query);
        self.__lock.release();

        ## Todos tem uma chance:
        count = 1;
        for entry in valRet:
            query  = "UPDATE HISTORIC SET valid=0 WHERE ";
            query += "timestamp='" + entry[0] + "' and "; 
            query += "token='"     + entry[1] + "' ";

            self.__lock.acquire();
            valRet = self.__mysql.update_query(query);
            self.__lock.release();

            ## LOOK:
            if entry[2] > self.__sMin:
                count += 1;

        return count;
## End.




class Scheduller:
    """
    Classe que realiza o agendamento das requisicoes de VM.
    ---------------------------------------------------------------------------
    """
    ###########################################################################
    ## ATTRIBUTES                                                            ##
    ###########################################################################
    __mysql   = None;
    __lock    = None;
    __division= None;


    ###########################################################################
    ## SPECIAL METHODS                                                       ##
    ###########################################################################
    def __init__(self, lock, mysql, division):
        buffer  = '[SCHEDULLER DIVISION ' + str(division) + '] ';
        buffer += 'Instanciando o Scheduller';
        logging.info(buffer);

        self.__division = division;
        self.__mysql    = mysql;
        self.__lock     = lock;


    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    ##
    ## BRIEF:
    ## ------------------------------------------------------------------------
    ##
    def execute(self, data):
        buffer  = '[SCHEDULLER DIVISION ' + str(self.__division) + '] ';
        buffer += 'Executando o Scheduller';
        logging.info(buffer);

        vList = self.__round_robin(data);
        return vList;


    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    ##
    ## BRIEF: realiza o escalonamento round robbin.
    ## ------------------------------------------------------------------------
    ## @@PARAM data == dados para o escalonamento.
    ##
    def __round_robin(self, data):
        myToken    = data[0].replace('"', '');
        vType      = data[1];

        playerList = [];

        query  = "SELECT token FROM STATUS WHERE ";
        query += "division='" + str(self.__division) + "' ";
        query += "ORDER BY score DESC";

        self.__lock.acquire();
        valRet = self.__mysql.select_query(query);    
        self.__lock.release();
         
        buffer  = '[SCHEDULLER DIVISION ' + str(self.__division) + '] ';
        buffer += 'Foram encontrados ' + str(valRet) + ' players.';
        logging.info(buffer);

        ## TODO: ver o que acontece.

        for entry in valRet:
            token = entry[0];

            ## Nao eh permitido escalonar a requisicao na origem da requisicao.
            if token == myToken:
                continue;

            query = "SELECT endpoint FROM PLAYER WHERE token='"+ entry[0] +"'";

            self.__lock.acquire();
            valRet = self.__mysql.select_query(query);    
            self.__lock.release();

            endpoint = (valRet[0])[0];

            ## Cria uma lista com os players aptos a executarem a requisicao de
            ## instancia de VM.
            playerList.append({
               "token"   : token,    
               "endpoint": endpoint,
               "vm_type" : vType
            });

        return playerList; 
## End.








class Referee_division(Process):
    """
    Classe que representa uma divisao. 
    ---------------------------------------------------------------------------
    """
    ###########################################################################
    ## ATTRIBUTES                                                            ##
    ###########################################################################
    __tRount     = None;
    __queue      = None;
    __scheduller = None;
    __attributes = None;
    __division   = None;


    ###########################################################################
    ## SPECIAL METHODS                                                       ##
    ###########################################################################
    def __init__(self, division, tRound, queue, lock, mysql, sMin, sMax):
        super(Referee_division, self).__init__();

        buffer = '[DIVISION ' + str(division) + '] instanciando a divisao.';
        logging.info(buffer);

        self.__tRound   = tRound;
        self.__queue    = queue;
        self.__division = division;

        ## Instancia a classe responsavel por realizar o calculo do score e da
        ## reputacao do player e do escalonador das VMs.
        self.__attributes = Attributes(lock, mysql, division, sMin, sMax);
        self.__scheduller = Scheduller(lock, mysql, division);



    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    ##
    ## BRIEF: executa o loop principal.
    ## ------------------------------------------------------------------------
    ##
    def run(self):
        buffer = '[DIVISION ' + str(self.__division)+ '] Executando a divisao.';
        logging.info(buffer);

        ## Tempo base p realizar o calculo inicial do tempo p o primeiro round.
        dOld = datetime.datetime.now();

        data = '';
        while True:
           try:
               if   self.__division == 3:
                   data = self.__queue[0].get(block=False);

               elif self.__division == 2:
                   data = self.__queue[1].get(block=False);

               elif self.__division == 1:
                   data = self.__queue[2].get(block=False);
           except:
               pass;

           if data:
               ## Recebe uma lista ordenada de players aptos a receberem a re-
               ## quisicao de instancia de VM de um determinado tipo.
               playerList = self.__scheduller.execute(data);

               for p in playerList:
                   try:
                       h = {'Content-type': 'application/json'};
                       d = {'auth_token': p["token"], 'type': p["vm_type"]};
                       u = p['endpoint'] + '/Vm';
                       r = requests.post(u, data=json.dumps(d), headers=h);
                   except:
                       buffer  = '[DIVISION ' + str(self.__division) + '] '
                       buffer += 'Falha de comunicacao com ' + p['endpoint'];
                       logging.info(buffer);
                       continue;

                   buffer  = '[DIVISION ' + str(self.__division) + '] '
                   buffer += 'Requisicao de ' + p['token'] + ' foi ' + r.text; 
                   logging.info(buffer);

                   ## Atualiza o status do player: 'vm accept' ou 'vm reject'.
                   self.__attributes.update(p["token"], r.text, p["vm_type"]);

                   if r.text == 'accept':
                       buffer  = '[DIVISION ' + str(self.__division) + '] '
                       buffer += 'Instancia de VM criada em ' + p['endpoint'];
                       logging.info(buffer);
                       break;
                    
                    ## TODO:tratar o caso de nenhum player aceitar a requisicao
               data = '';

           ## Captura o tempo atual p/ calcular se o tempo decorrido compreende
           ## o espaco de tempo necessario para a execucao de um ROUND.
           dNow = datetime.datetime.now();
           
           ## Tempo decorrido do ultimo ROUND ate o momento atual. Utilizado p/
           ## verificar o proximo ROUND.
           elapsedTime = dNow - dOld;

           if divmod(elapsedTime.total_seconds(), 60)[0] >= self.__tRound:
               ## Realiza o calculo dos atributos de todos os players presentes
               self.__attributes.calculate();
               dOld= datetime.datetime.now();

        return 1;
## End.




class Referee_core(Process):
    """
    Classe core do referee.
    ---------------------------------------------------------------------------
    """
    ###########################################################################
    ## ATTRIBUTES                                                            ##
    ###########################################################################
    __mysqlcnx = None;
    __lock     = None;
    __queue    = None;


    ###########################################################################
    ## SPECIAL METHODS                                                       ##
    ###########################################################################
    def __init__(self, queue, lock, mysql):
        super(Referee_core, self).__init__();

        buffer = '[REFEREE CORE] Instanciando o Referee Core.';
        logging.info(buffer);

        self.__queue = queue;
        self.__mysql = mysql;
        self.__lock  = lock;


    ###########################################################################
    ## PUBLIC METHODS                                                        ##
    ###########################################################################
    ##
    ## BRIEF: executa o referee core.
    ## ------------------------------------------------------------------------
    ##
    def run(self):
        buffer = '[REFEREE CORE] Executando o Referee Core.'
        logging.info(buffer);
        valRet = 1;

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1);

        s.bind(("127.0.0.1", INTERNAL_PORT));
        s.listen(5);

        while True:
           ## Espera por novas conexoes e dados p executar tarefas apropriadas.
           connection, address = s.accept();

           message = connection.recv(1024);

           ## A message 000 significa shutdown:.
           if   message == "000":
               break;
           else:
               codOper = message.split("|")[0];
               payload = message.split("|")[1];

               ## Codigo 001 refere-se ao cadastramento de um novo player ou re
               ## torno de um token caso ele exista.
               if   codOper == "001":
                   buffer = '[REFEERE CORE] Add um novo player ' + str(payload);
                   logging.info(buffer);
                   valRet = self.__add_player(payload);

               ## Codigo 002 refere-se ao descadastramento de um player do MCT.
               elif codOper == "002":
                   buffer = '[REFEERE CORE] Del um novo player ' + str(payload);
                   logging.info(buffer);
                   valRet = self.__del_player(payload);

               ## Codigo 003 refere-se ao pedido de instanciacao de uma nova VM
               elif codOper == "003":
                   buffer = '[REFEERE CORE] Requisita uma VM '  + str(payload);
                   logging.info(buffer);
                   valRet = self.__new_virtual(payload);
               
               connection.sendall(valRet);
               connection.close();

        connection.close();
        return 1;


    ###########################################################################
    ## PRIVATE METHODS                                                       ##
    ###########################################################################
    ##
    ## BRIEF: adiciona um player ao MCT.
    ## ------------------------------------------------------------------------
    ## @PARAM payload == os dados do player. 
    ##
    def __add_player(self, payload):
        f1 = payload.split(",")[0];
        f2 = payload.split(",")[1];
        f3 = payload.split(",")[2];
        f4 = payload.split(",")[3];
        f5 = payload.split(",")[4];
        f6 = payload.split(",")[5];  
        f7 = payload.split(",")[6];  
 
        query  = "SELECT token FROM PLAYER WHERE ";
        query += "name='" + str(f1) + "' AND endpoint='" + str(f2) + "'";

        self.__lock.acquire();
        valRet = self.__mysql.select_query(query);
        self.__lock.release();
  
        ## Caso tenha retorno signigica que o player jah esta cadastrado. Dian-
        ## te disso nao eh necessario inseri-lo de volta.
        if len(valRet) != 0:
           ## TODO vertificar o tipo do campo, se parece com um token !!!!!!!!!
           authToken = (valRet[0])[0];
           
        else:
           ## Cria uma hash de autenticacao para o novo player que acabou de pe
           ## dir para se autentitcar. 
           authToken = hashlib.sha1(os.urandom(128)).hexdigest();

           query=("INSERT INTO PLAYER "
                  "(token, name, endpoint, os, plataform, hypervisor, country, max_division) "
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)");

           value = (authToken, f1, f2, f3, f4, f5, f6, f7);

           self.__lock.acquire();
           self.__mysql.insert_query(query, value);    
           self.__lock.release();

           ## Eh necessario calcular o score e a reputacao inicial p/ que o no-
           ## vo player nao tenha desvantagem no MCT.
           buffer = '[REFEERE CORE] Calcula atributos iniciais: ' + authToken;
           logging.info(buffer);

           attributes = Attributes(self.__lock, self.__mysql, 3, 0.1, 0.1);
           score      = attributes.initial_atributes();

           ## Inicia a tabela de status para o novo player inserido no ambiente
           query = ("INSERT INTO STATUS "
                    "(token, division, score, playoff) "
                    "VALUES (%s, %s, %s, %s)");

           value = (authToken, 3, score, 0);

           self.__lock.acquire();
           self.__mysql.insert_query(query, value);    
           self.__lock.release();
    
        return authToken;    


    ##
    ## BRIEF: remove um player do torneio.
    ## ------------------------------------------------------------------------
    ## @PARAM payload == os dados do player. 
    ##
    def __del_player(self, payload):
        ## Situacao onde o player pede para sair do torneio. Outra possibilida-
        ## de eh ele ser eliminado na classe Status.. 
        authToken = payload.split(",")[0];

        query = "DELETE FROM PLAYER WHERE token='" + str(authToken) + "'";

        self.__lock.acquire();
        valRet = self.__mysqlcx.delete_query(query);    
        self.__lock.release();

        return "Unsubscribe!";


    ##
    ## BRIEF: envia um pedido de criacao de uma nova maquina virtual.
    ## ------------------------------------------------------------------------
    ## @PARAM payload == os dados do player. 
    ##
    def __new_virtual(self, payload):
        ## Auth_token e tipo de maquina virtual.
        authToken = payload.split(",")[0];
        typeVm    = payload.split(",")[1];

        ## Descobre a divisao do player para ver se ele esta apto a enviar re-
        ## quisicoes de determinados tipos de VMs.
        query  = "SELECT division FROM STATUS WHERE token='" + authToken + "'";

        self.__lock.acquire();
        valRet = self.__mysql.select_query(query);
        self.__lock.release();
        
        playerDivision = (valRet[0])[0];

        ## Verifica se o player esta apto para enviar a requisicao p a divisao.
        ## Um player na divisao 3 soh pode enviar requisicoes do tipo 'T'.
        ## Um player na divisao 2 soh pode enviar requisicoes do tipo 'T,S'.
        ## Um player na divisao 1 soh pode enviar requisicoes do tipo 'T,S,B'.
        if   typeVm == 'T':
            self.__queue[0].put([authToken, typeVm]);

        elif typeVm == 'S' and playerDivision != 3:
            self.__queue[1].put([authToken, typeVm]);

        elif typeVm == 'B' and playerDivision == 1:
            self.__queue[2].put([authToken, typeVm]);
        else:
             buffer  = '[REFEREE CORE] O player ' + authToken + ' ';
             buffer += 'na divisiao ' + str(playerDivision)   + ' ';
             buffer += 'nao pode requerer a VM tipo ' + typeVm;
             logging.info(buffer);
             return 'reject';


        ## TODO: tratar volta - talvez utilizar outra tipo de comunicacao e nao
        ##       o Queue. Talvez criar um objeto com uma fila de dicionario.
        return 'accept';
## End.




###############################################################################
## MAIN                                                                      ##
###############################################################################
if __name__ == "__main__":
    ## Configuracoes diversas:
    ## --------------------------------------------------------------------- ##
    ## Executa o config parse para recuperar os parametros referente a execucao
    ## das divisoes.
    config = ConfigParser.RawConfigParser();
    config.read('config.ini');

    lFile = config.get("main", "log_file"); 

    dhost = config.get('mysqld', 'host');
    duser = config.get('mysqld', 'user');
    dpass = config.get('mysqld', 'pass');
    dbase = config.get('mysqld', 'base');

    ## Contrato:
    ## --------------------------------------------------------------------- ##
    tla = ConfigParser.RawConfigParser();
    tla.read('tla.ini');

    tRound3 = tla.getfloat('division3', 'time_round');
    sMin3   = tla.getfloat('division3', 'score_min' );
    sMax3   = tla.getfloat('division3', 'score_max' );

    tRound2 = tla.getfloat('division2', 'time_round');
    sMin2   = tla.getfloat('division2', 'score_min' );
    sMax2   = tla.getfloat('division2', 'score_max' );

    tRound1 = tla.getfloat('division1', 'time_round');
    sMin1   = tla.getfloat('division1', 'score_min' );
    sMax1   = tla.getfloat('division1', 'score_max' );

    ## Inicia o objeto responsavel pelo log e definie como sera o prefixo das
    ## entradas gravadas no arquivo.
    format_log='%(asctime)s %(message)s';
    logging.basicConfig(filename=lFile, level=logging.INFO, format=format_log);
    logging.info('## --------------------------------##');
    logging.info('## [MAIN] Inicia o Referee! ;)     ##');
    logging.info('## --------------------------------##');

    ## Esse lock eh utilizado p/ manter a sincronizacao de escrita no database.
    lock = Lock();

    ## O queue eh utilizado p/ realizar a comunicacao entre processos executa-
    ## dos em thread.
    queue = [Queue(), Queue(), Queue()];

    try:
        ## TODO: colocar o lock aqui.
        mysqlcnx = Database(dhost, duser, dpass, dbase);
    except:
        sys.exit(1);

    ## Executa em thread o core do referee, a comunicacao sera realizada inici-
    ## almente por unix socket, mais tarde mudar para AMQP.
    referee = Referee_core(queue, lock, mysqlcnx);
    referee.start();
  
    div3 = Referee_division(3, tRound3, queue, lock, mysqlcnx, sMin3, sMax3);
    div3.start();

    div2 = Referee_division(2, tRound2, queue, lock, mysqlcnx, sMin2, sMax2);
    div2.start();

    div1 = Referee_division(1, tRound1, queue, lock, mysqlcnx, sMin1, sMax1);
    div1.start();

    ##
    os.environ['ENDPOINTS_SIMPLE_HOST']='localhost:50000';
    os.environ['ENDPOINTS_PREFIX']     = 'referee_controller';

    try:
       ## Inicia o player como um webserver:
       referee_server = Server();
       referee_server.serve_forever();

    ## Caso Ctrl-C seja precionado realiza os procedimentos p/ finalizar o ambi
    ## ente.
    except KeyboardInterrupt, e:
        pass;

    referee.terminate();

    div3.terminate();
    div2.terminate();
    div1.terminate();

    logging.info('## --------------------------------##');
    logging.info('## [MAIN] Finalizando o Referee :( ##');
    logging.info('## --------------------------------##');
## End.
