Application reading CSV file from main director (from attached file size I decide to locate it inside project not send by http request)
for saving file data I choose PostgresSQL because I'm more experienced with relational db's especially ORACLE and PostgresSQL.
It's my first experience with Go, so I tried my best :)

● The .csv file could be very big (billions of entries) - how would your application
perform?
I think use a distributed architecture and splitting the CSV file into smaller chunks, and processing them in parallel on multiple machines. 

● Every new file is immutable, that is, you should erase and write the whole storage;
(this part I decide to create job and every 30 min truncate table and insert from new CSV file in real-life application 
this db updating periods (in case of db replication aI think we need ) 

● How would your application perform in peak periods (millions of requests per
minute)?
To handle peak periods, we can scale the application horizontally by deploying multiple instances of
the application behind a load balancer. This approach helps distribute the load among multiple instances of the application,
so in case of huge volume we can use sharding technique and divide file by portions and save it in different db's and
from request ID  decide where to route request based on previously partitioned data (in our case we are retrieving data by ID, and it automatically indexed)

● How would you operate this app in production (e.g. deployment, scaling, monitoring)?
Deploy the application to a cloud platform such as AWS, GCP or etc,
Use a load balancer to distribute incoming traffic across multiple instances of the application,
configure auto-scaling rules to automatically add or remove instances based on CPU utilization, memory usage, or other metrics.
Set up monitoring and alerting to detect issues before they become critical.
This could include using tools like Prometheus, Grafana to monitor metrics like CPU usage,
memory usage, and HTTP response times, also use ELK for centralized logging