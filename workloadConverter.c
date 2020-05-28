#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* Nessa versão temos a adição de puxar o endereço do requisitor para usar no algoritmo do SCORe mais para frente. Versão 5. */

/* CONVERSOR DE WORKLOAD DA GRID5K PARA FORMATO .JSON
    AUTOR: GEREMIAS CORRÊA
    REALIZADO EM MAIO DE 2020

    DADOS QUE PRECISAM SER AJUSTADOS CASO VÁ USAR:
    velocidade_host: setar com o valor médio baseado no FLOP/s de seu sítio.
    hosts_disponiveis: baseado na quantidades de nós totais de seu sítio.
    As localizações das leituras dos arquivos e o nome de geração (caso queira)
    O periodo de tempo analisado, setado as variaveis limite de corte de subtime baseado no timestamp no Unix.
        Todas essas variáveis estão globais, para faciltiar a identificação.

    Caso leia o arquivo jobs.csv de Lille, por exemplo, com 3,2kk de linhas, demora cerca de 1 minuto atualmente.
*/

//Resultante de Grenoble na velocidade média dos hosts:
//Média = ((32 * 428,9 GF) + (4 * 847,2 GF))/36 = 475,38 GF 

//Resultante de Lille na velocidade média dos hosts:
//Na ordem: chifflet, chifflot, chiclet e chetemi:
//media =  (419,5 * 8) + (408,9* 8) + (380,1 * 8) + (331,4 * 15) = 375,36 GF - sim, refazer os testes

long double velocidade_host = 375.36; //Para calcular a quantidade de processamento necessária
//usar 375,36, mas transformar tudo pra float depois pra não dar problema -- VERRRRRRRR

int const hosts_disponiveis = 39; //Quantidade de nós disponíveis do sítio da plataforma.

long long TAM = 60000;
//TAM é usado para definir o tamanho da struct, vetor de profiles e também quantos dados limites eu quero

/* SEGUNDOS EM CADA PERÍODO DE TEMPO:
1 DIA = 86400
7 DIAS = 604800
1 MÊS = 2592000
1 ANO = 31536000
*/

//Aqui deixo claro o período que quero buscar
//1588704468 = Tue May 5 15:47:48 2020
//1588647920 é 5/5/20 às 00h00. Para ter base

/*Primeiro pegando 1/4/20 até 1/5/20, então: 
    subtimeMax = 1588647920 - 432000 = 1588215920
    subtimeMin = 1588647920 - 3024000 = 1585623920
*/
/* Periodo 1/1/20 até 1/5/20, então:
    subtimeMax = 1588215920
    subtimeMin = 1577847920 
*/
/* Periodo 1/1/19 até 1/1/20, então:
    subtimeMax = 1577847920
    subtimeMin = 1546311920
*/
/* Periodo 1/1/18 até 1/1/19:
    subtimeMax = 1546311920
    subtimeMin = 1514775921
*/

long long subtimeMax = 1588215920;
long long subtimeMin = 1577847920;

/* Onde está cada campo da struct:
    id: campo 1 do jobs.csv (e em outros, mas uso de comparação para inserir ou não na struct)
    subtime: campo 21 do jobs.csv
    res: quantidade linhas com medo id do arquivo assigned_resources.csv
    walltime: campo 3 do moldable_job_descriptions.csv
    cpuprofile: campo 23 - campo 22 do jobs.csv. Isso multiplicado ao velocidade_host, velocidade média dos hosts da plataforma lida.
    - obs: deixei a variável velocidade_host como global, pois essa precisa ser alterada na mão.
*/

struct Jobs{//não adianta usar float porque no json não usamos
    unsigned int id; //id do problema - campo 1 do jobs.csv/db_jobs e 1 do 
    long long subtime; //tempo de submissão - campo 21 do jobs.csv/db_jobs - é em relação ao primeiro subtime de job
    unsigned short int res; //qtd de recursos(hosts) requisitadas - linhas do arquivo assigned_resources.csv
    unsigned int walltime; //tem requisitado esperado de execução 
    unsigned long int cpuprofile; //flop de processamento para execução
    unsigned short score; //0 a 100 - não utilizado ainda
    char *endereco; //endereço do requisitador, para podermos repetir o usuário no escalonemento
};
    
int main(){
    struct Jobs jobs[TAM]; //Ex: se TAM = 200000, entaõ 28 bytes * 200000 = 5,6kk de bytes = 5,6Mb
    int i,j;

    //ABERTURA ARQUIVO 1 - jobs.csv ou db_jobs.csv
    //Dados que interessam no jobs.csv: ID (1); submission time (21); start_time(22) e stop_time (23) para cpu profile

    //Variáveis usadas para leitura do arquivo 1 e transformação dos dados para a struct
    char c; //caractere da leitura do arquivo
    char palavra[1000]; //string auxiliar para receber palavra do arquivo
    int contadorLetra = 0; //conta a letra atual da palavra na leitura
    int iniciopalavra = 1; //se é ou não início de palavra
    int linhaLeitura1 = 0; //linha atual (e posteriormente máxima) que está a leitura
    int campo = 1; //coluna, para saber qual dado interessa.
    long long stop = 0, start = 0;

    //Variáveis para a parte que pego o período de tempo e só insiro se válido
    unsigned int auxiliarId = 0;
    long long auxiliarSubtime = 0;
    long double auxiliarCpuprofile = 0;
    int contadorInseridos = 0;
    char auxiliarEndereco[1200];

    //Variáveis para criação de profiles para jogar no .JSON
    unsigned long int vetorProfiles[TAM]; //Vetor que tem todos os valores brutos de flop de profiles
    vetorProfiles[0] = 0; //Garanto que esse profile já está inserido
    int contProfiles = 1; //contador atual da quantidade preenchidos
    int contido; //Flag para saber se o profile já está contido no vetor
    
    FILE *arq;
    arq = fopen("newdb Lille/jobs.csv","r");
    if(arq){
        while(1){
            if(contadorInseridos > TAM){ //Aqui é o valor do set da qtd de structs acessíveis
                printf ("Struct cheia\n");
                break;
            }
            c = fgetc(arq);
            if (feof(arq)) {
                printf ("Lendo o 1\n\n");
                break;
            }

            if(iniciopalavra == 1 && c != ',' && c != '\n'){// Inicio palavra
                palavra[0] = '\0'; //para limpar
                contadorLetra = 0;
                iniciopalavra = 0;
                palavra[contadorLetra] = c;
                contadorLetra++;
            }
            else if (c == ',' || c== '\n' ){ //Fim palavra
                palavra[contadorLetra] = '\0';
                iniciopalavra=1;
                
                if(campo == 1){//id
                    auxiliarId = atoi(palavra);
                    
                }
                if(campo == 20){//endereço
                    strcpy(auxiliarEndereco, palavra);
                }
                else if (campo == 21){ //Subtime
                    //Caso queira para teste de algoritmo dá para setar todos os subtimes em 1 (igual)    
                    auxiliarSubtime = atol(palavra);
                }
                else if (campo == 22) //start_time
                    start = atol(palavra);
                else if (campo == 23){ //stop_time e então cpuprofile
                    stop = atol(palavra);
                    auxiliarCpuprofile = (stop-start) * velocidade_host;
                    if(auxiliarCpuprofile < 0){ //não teve stop_time
                        auxiliarCpuprofile = 0;
                    }
                }
                //Caso existam mais campos necessários, inserir aqui como else if

                campo++;
                if(c == '\n'){ //fim da linha
                    //printf ("%d %s\n", auxiliarId, auxiliarEndereco);
                    if(campo >= 33 && campo <= 34 && auxiliarCpuprofile > 0 && auxiliarId > 0){
                            if(auxiliarSubtime >= subtimeMin && auxiliarSubtime <= subtimeMax){ //Se sim, então insere
                                //Inserindo o Job
                                printf ("INSERIDO %d: %d %s(tam %lu)\n", contadorInseridos, auxiliarId, auxiliarEndereco, (strlen(auxiliarEndereco)+1));
                                jobs[linhaLeitura1].id = auxiliarId;
                                jobs[linhaLeitura1].subtime = auxiliarSubtime;
                                jobs[linhaLeitura1].cpuprofile = (long long int)auxiliarCpuprofile;
                                
                                /*
                                for(i=0; i<strlen(auxiliarEndereco); i++){
                                    if(auxiliarEndereco[i] == '/')
                                        contadorBarras++;
                                    else if(contadorBarras == 2){
                                        nomeUsuario[contadorLetraEndereco] = auxiliarEndereco[i];
                                        contadorLetraEndereco++;
                                    }
                                    if(contadorBarras == 3 || i == strlen(auxiliarEndereco) - 1){
                                        nomeUsuario[contadorLetraEndereco] = '\0';
                                        break;
                                    }
                                }
                                */

                                jobs[linhaLeitura1].endereco = (char *)malloc(strlen(auxiliarEndereco) * sizeof(char) + 1);
                                strcpy(jobs[linhaLeitura1].endereco, auxiliarEndereco);
                                linhaLeitura1++;
                                contadorInseridos++;

                                //Também inserindo o profile
                                contido = 0;
                                for(i = 0; i < contProfiles; i++){
                                    if((unsigned long int)auxiliarCpuprofile == vetorProfiles[i]){
                                        contido = 1;
                                        break;
                                    }
                                }
                                if (contido == 0){
                                    vetorProfiles[contProfiles] = (unsigned long int)auxiliarCpuprofile;
                                    contProfiles++;
                                }
                            }
                    }
                    campo =1;                        
                }
            }
            else{//caractere 2 ao último da palvra
                palavra[contadorLetra] = c;
                contadorLetra++;
            }
        }
    }
    else{
        printf("Erro ao abrir o arquivo\n");
        return 1;
    }
    fclose(arq);

    /* ARRUMANDO OS SUBTIMES PARA O MENOR SUBTIME SER = 1 (INÍCIO DA PLATAFORMA):*/
    //Também para saber quantos dias de execução estou analisando.
    //SE QUISER CALCULAR AQUI TAMBÉM COISAS COMO MÉDIA DE TMEPO DE WALLTIME, CPUPROFILE, ETC
    //Funcionando.
    long long min = jobs[0].subtime;
    long long max = min;
    for(i=1; i<linhaLeitura1; i++){
        if(jobs[i].subtime < min)
            min = jobs[i].subtime;
        if(jobs[i].subtime > max)
            max = jobs[i].subtime;
    }
    long long qtdSegundosTotal = max - min;
    
    //Agora modificando cada um deles com o menor existente (min)
    for(i=0; i<linhaLeitura1; i++)
        jobs[i].subtime = jobs[i].subtime - min + 1; //garanto que será no mínimo 1
    /*---*/

    //LEITURA DO ARQUIVO 2 - moldable_job_descriptions.csv
    //Nele temos a variável walltime que nos interessa 
    contadorLetra=0;
    iniciopalavra=1;
    char palavraMenor[100]; //não preciso de grande no arq2 e 3
    int linhaLeitura2 =-1; //para não confundir com o linha, pois podem ter quantidades diferentes no arquivo de IDs
    campo = 1; //coluna, para saber qual dado interessa.
    int auxId = 0; //para garantir a id de qual devo mandar o walltime
    
    FILE *arq2;
    arq2 = fopen("newdb Lille/moldable_job_descriptions.csv","r");
    if(arq2){
        while(1){
            c = fgetc(arq2);
            if (feof(arq2)) {
                printf ("Lendo o 2\n\n");
                break;
            }

            if(linhaLeitura2 != -1){ //não ser primeira linha, explicando as variáveis. Fiz p/ não precisar ficar editando o .csv
                if(iniciopalavra == 1 && c != ',' && c!= '\n'){
                    palavraMenor[0] = '\0'; //para limpar
                    contadorLetra = 0;
                    iniciopalavra = 0;
                    palavraMenor[contadorLetra] = c;
                    contadorLetra++;
                }
                else if (c == ',' || c== '\n' ){ //fim palavra
                    palavraMenor[contadorLetra] = '\0';
                    iniciopalavra=1;
                    
                    if(campo == 1){//id
                        auxId = atoi(palavraMenor);
                    }
                    else if (campo == 3){
                        for (i = 0; i < linhaLeitura1; i++){ //linha é a qtd existente na leitura 1
                            if(auxId == jobs[i].id){
                                jobs[i].walltime = atoi(palavraMenor); //por enquanto somente um inteiro, tem atol se precisar
                                break;
                            }
                        }
                    }

                    campo++;
                    if(c == '\n'){ //fim da linha2
                        campo = 1;
                        linhaLeitura2++;
                    }
                }
                else{//caractere 2 ao último da palvra
                    palavraMenor[contadorLetra] = c;
                    contadorLetra++;
                }
            }
            else if(linhaLeitura2 == -1 && c == '\n') //primeira linha2, tutorial das variáveis
                linhaLeitura2++;
        }
    }
    else{
        printf("Erro ao abrir o arquivo\n");
        return 1;
    }
    fclose(arq2);

    //LEITURA DO ARQUIVO 3 - assigned_resources.csv
    //Nele temos a quantidade de recursos solictados
    //está pedindo recurso específico, vamos apenas somar a quantidade de host solicitadas em um mesmo id e lanças na struct
    int linhaLeitura3 = -1; //Linha atual da leitura 3
    contadorLetra=0; //já mencionado
    iniciopalavra=1; //já mencionado
    campo = 1; //coluna, para saber qual dado interessa.
    int ultimaID = 1; //Ultima id lida (linha anterior) do arquivo
    int qtdRes = 0; //Quantidade atual de hosts requisitados da ID atual.
    
    //Como muitos dados alguns não deixam claro quantos recursos requisitados, setar antecipadamente todos para 1.
    for(i = 0; i<linhaLeitura1; i++)
        jobs[i].res = 1;
    
    FILE *arq3;
    arq3 = fopen("newdb Lille/assigned_resources.csv","r"); //as primeiras 1000 IDs por enquanto
    if(arq3){
        while(1){
            c = fgetc(arq3);
            if (feof(arq3)) {
                printf ("Lendo o 3\n\n");
                break;
            }
            if(linhaLeitura3 != -1){ //não ser primeira linha, explicando as variáveis. Fiz p/ não precisar ficar editando o .csv
                if(iniciopalavra == 1 && c != ',' && c!= '\n'){
                    palavraMenor[0] = '\0'; //para limpar - teste
                    contadorLetra = 0;
                    iniciopalavra = 0;
                    palavraMenor[contadorLetra] = c;
                    contadorLetra++;
                }
                else if (c == ',' || c== '\n' ){ //fim palavra
                    palavraMenor[contadorLetra] = '\0';
                    iniciopalavra=1;
                    if(campo == 1){//id
                        if(ultimaID == atoi(palavraMenor)){//se ultima id igual a atual, continuo somando
                            qtdRes++;
                        }
                        else{ //Caso tenha mudado a id, não temos mais requisição de hosts nela
                            for (i = 0; i < linhaLeitura1; i++){ //para garantir que existe essa id
                                if(ultimaID == jobs[i].id){
                                    if(qtdRes > hosts_disponiveis)
                                        jobs[i].res = 39; //por enquanto somente um inteiror
                                    else
                                        jobs[i].res = qtdRes;
                                    break;
                                }
                            }
                            qtdRes=1; //seta em 1 porque a primeira aparição da id já é agora
                            ultimaID = atoi(palavraMenor);
                        }
                    }
                    campo++;
                    if(c == '\n'){ //fim da linha2
                        campo = 1;
                        linhaLeitura3++;
                    }
                }
                else{//caractere 2 ao último da palvra
                    palavraMenor[contadorLetra] = c;
                    contadorLetra++;
                }
            }
            else if(linhaLeitura3 == -1 && c == '\n') //primeira linha2, tutorial das variáveis
                linhaLeitura3++;
        }
    }
    else{
        printf("Erro ao abrir o arquivo\n");
        return 1;
    }
    fclose(arq3);
    
    free(arq);
    free(arq2);
    free(arq3);

    //PRINT PARA AMOSTRAGEM NO TERMINAL APENAS:
    printf ("ID \t Endereco \t\tSubtime \t\t Flop(G) \t\t Walltime \t\t Recursos Req\n");
    for(j=0; j< linhaLeitura1; j++){ //linha é baseado no primeiro arquivo, onde conto a qtd de IDs
        printf ("%u \t %s \t\t%lld \t\t %lu \t\t %u \t\t %hu\n", jobs[j].id, jobs[j].endereco,jobs[j].subtime, jobs[j].cpuprofile, jobs[j].walltime, jobs[j].res);
    }
    printf ("Quantidade de jobs cadastrados: %d\n", linhaLeitura1);
    printf ("Tempo total analisado: %lld segundos ou %lld dias\n", qtdSegundosTotal, qtdSegundosTotal/86400);
    
    /* ARQUIVO DE SAÍDA NO MEU FORMATO JSON: */

    //SALVANDO JOBS:
    FILE *arq4;
    arq4 = fopen("newdb Lille/testeComEndereco.json","w");
    fprintf (arq4, "{\n");//primeira linha
    fprintf (arq4, "\t\"nb_res\": 39,\n");
    fprintf (arq4, "\t\"jobs\": [\n"); //inicia os jobs
    for (i = 0; i<linhaLeitura1; i++){ //insere cada job
        fprintf(arq4,"\t\t{\n");
        fprintf(arq4,"\t\t\t\"id\": \"%u\",\n", jobs[i].id);
        fprintf(arq4,"\t\t\t\"res\": %hu,\n", jobs[i].res);
        fprintf(arq4,"\t\t\t\"subtime\": %lld,\n", jobs[i].subtime);
        if(jobs[i].walltime == 0) //Se 0, seta 1 hora, como pedido pelo professor
            fprintf(arq4,"\t\t\t\"walltime\": 3600,\n");
        else
            fprintf(arq4,"\t\t\t\"walltime\": %u,\n", jobs[i].walltime);
        fprintf(arq4,"\t\t\t\"profile\": \"%lu\",\n", jobs[i].cpuprofile); //arrumar ainda        
        fprintf(arq4,"\t\t\t\"user\": \"0\",\n");
        jobs[i].score = rand() % 101; //gerando score aleatório de 0 a 100 (ainda sem uso)
        //Caso queira atribuir valores metodológicos, é necessário modificá-lo (preferencialmente antes daqui)
        fprintf(arq4,"\t\t\t\"score\": %hu,\n",jobs[i].score);
        fprintf(arq4,"\t\t\t\"address\": \"%s\"\n", jobs[i].endereco);
        if(i != linhaLeitura1 - 1)
            fprintf(arq4,"\t\t},\n");
        else
            fprintf(arq4,"\t\t}\n");
        //testar se preciso adaptar o numero do profile para menor que o original setado na variavel
    }
    fprintf (arq4, "\t],\n"); //fecha os jobs, com vírgula pq ainda tem os profiles.
    
    //SALVANDO PROFILES:
    fprintf(arq4, "\t\"profiles\": {\n"); //abre profiles
    for(i = 0; i < contProfiles; i++){ //insere cada profile de job 
        fprintf (arq4,"\t\t\"%lu\": {\n", vetorProfiles[i]);
        if(i==0)   
            fprintf (arq4,"\t\t\t\"cpu\": %lu,\n", vetorProfiles[i]);//multiplicação necessária pois está em giga
        else
            fprintf (arq4,"\t\t\t\"cpu\": %lu000000000,\n", vetorProfiles[i]);//multiplicação necessária pois está em giga
        fprintf (arq4,"\t\t\t\"com\": 0,\n");
        fprintf (arq4,"\t\t\t\"type\": \"parallel_homogeneous\"\n");
        if(i != contProfiles - 1)
            fprintf (arq4, "\t\t},\n");
        else
            fprintf(arq4, "\t\t}\n");
    }
    fprintf(arq4, "\t}\n"); //fecha profiles
    fprintf (arq4,"}\n"); //fim de arquivo
    fclose(arq4);
    free(arq4);
    for(i=0; i<linhaLeitura1; i++)
        free(jobs[i].endereco);
    return 0;
}

