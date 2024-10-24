🔧🚀Fast Data Architecture 🚀🔧

![Arquitetura](https://github.com/rodrigofjorge77/FastDataArchitecture-Kafka-Flink/blob/main/assets/architecture.png)

📊 Como funciona? Utilizando Python, extraí os dados de vendas de uma API na nuvem e os enviei para um tópico no Apache Kafka. 
A partir daí, o Apache Flink processa esses dados em tempo real diretamente do tópico Kafka, realizando análises e transformações necessárias. 
O resultado final é salvo em uma tabela do PostgreSQL e, como toque final, um dashboard no Power BI está conectado em tempo real com essa tabela, 
fornecendo insights atualizados instantaneamente para tomada de decisão rápida e precisa!

Esse pipeline otimiza o fluxo de dados e garante que as informações estejam sempre atualizadas para os times de negócio.

![Arquitetura](https://github.com/rodrigofjorge77/FastDataArchitecture-Kafka-Flink/blob/main/assets/output.gif)

##English

How does it work? Using Python, I extracted sales data from a cloud-based API and sent it to a topic on Apache Kafka. From there, Apache Flink processes the data in real time directly 
from the Kafka topic, performing necessary analysis and transformations. The final result is saved in a PostgreSQL table, and as a finishing touch, a Power BI dashboard 
is connected in real time to this table, providing instant, up-to-date insights for quick and precise decision-making!

This pipeline optimizes the data flow and ensures that the information is always up-to-date for business teams.
