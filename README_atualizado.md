
# Projeto: Análise Paralela de Dados IoT com Pthreads

Este projeto tem como objetivo processar registros de sensores IoT a partir de um arquivo CSV, utilizando `pthreads` para distribuir a carga de trabalho entre os núcleos disponíveis no sistema. O programa calcula estatísticas mensais por sensor e por dispositivo e salva os resultados em um novo arquivo CSV.

---

## Objetivo

Ler uma base de dados contendo registros de sensores IoT e calcular, para cada dispositivo e mês, os valores mínimo, máximo e médio dos seguintes sensores:
- Temperatura
- Umidade
- Luminosidade
- Ruído
- eCO2
- eTVOC

---

## Estrutura do CSV de entrada

O arquivo deve ter os seguintes campos, separados por ponto e vírgula:

```
id;device;contagem;data;temperatura;umidade;luminosidade;ruido;eco2;etvoc;latitude;longitude
```

São utilizados:
- `device`: identifica o sensor IoT
- `data`: usada para extrair o mês e filtrar a partir de 2024-03
- `temperatura`, `umidade`, `luminosidade`, `ruido`, `eco2`, `etvoc`: usados nos cálculos

Os campos `id`, `latitude` e `longitude` são ignorados.

---

## Compilação

No terminal Linux, compile com:

```
gcc -o sensor_analysis_pthreads sensor_analysis_pthreads.c -lpthread
```

---

## Execução

Após compilar, rode o programa com o caminho para o arquivo CSV:

```
./sensor_analysis_pthreads devices.csv
```

O arquivo `resultados.csv` será criado com os dados de saída.

---

## Uso de Threads

A função `sysconf(_SC_NPROCESSORS_ONLN)` detecta automaticamente o número de núcleos do sistema. O programa então cria uma thread para cada núcleo disponível.

A estrutura `ThreadArgs` define os parâmetros que cada thread usa:
- `data`: ponteiro para os dados
- `start` e `end`: índices de início e fim da fatia de dados
- `local_stats`: ponteiro para o vetor de resultados locais da thread
- `local_count`: quantidade de grupos estatísticos locais processados

Cada thread é criada usando `pthread_create`, chamando a função `thread_worker`, que:
- percorre sua fatia do vetor de registros
- calcula o mínimo, máximo, soma e contagem para cada sensor agrupado por `device` e `ano-mês`
- armazena os dados no vetor local de `SensorStats`

---

## Fusão dos Resultados

A `main` utiliza `pthread_join` para aguardar o término de todas as threads.

Depois, percorre os vetores locais gerados por cada thread e usa `is_same_group` para comparar grupos iguais e consolidar:
- o mínimo entre os valores mínimos locais
- o máximo entre os valores máximos locais
- a soma total e a contagem para cálculo da média

---

## Geração do CSV

A função `salvar_csv` recebe o vetor consolidado e escreve o arquivo `resultados.csv` com o formato:

```
device;ano-mes;sensor;valor_maximo;valor_medio;valor_minimo
```

---

## Concorrência

O programa evita problemas de concorrência ao garantir que:
- Cada thread usa seu próprio vetor de estatísticas (`local_stats`)
- Nenhuma variável é compartilhada entre threads durante o processamento
- A fusão dos dados ocorre apenas depois do término de todas as threads, em código sequencial

`race conditions e sobrescrita simultânea de memória`, foram evitados garantindo que cada thread processe apenas seus próprios dados locais. Como a fusão dos resultados acontece após o término das threads, não há risco de conflitos no acesso à memória.
---

## Tipo de Threads

As threads criadas são `pthreads`, que são implementadas como threads em nível de núcleo no Linux. Isso significa que são agendadas diretamente pelo sistema operacional, permitindo execução paralela real.

