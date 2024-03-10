# How to Run Docker DB
---

- Untuk menjalankan Data Source DB - Sales DB masukkan command `docker compose up -d`

    ```bash
    $ docker pull shandytp/amazon-sales-data-docker-db
    $ docker run -d -p 5555:5555 --name etl_test_extract shandytp/amazon-sales-data-docker-db:latest
    ```

- Untuk menjalankan Data Warehouse, masuk ke dalam directory `docker-db/data-warehouse/` setelah itu masukkan command `docker compose up -d`

    ```bash
    $ cd docker-db/data-warehouse
    $ docker compose up -d
    ```