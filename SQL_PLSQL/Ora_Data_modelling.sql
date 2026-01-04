-- Creating table to store book data retrieved from API

create table book_store(
    store_id NUMBER generated always as identity,
    title  VARCHAR2(4000),
    genre   VARCHAR2(100),
    ratings VARCHAR2(15) CHECK(ratings IN ('0','1','2','3','4','5','7','8','9')),
    price   NUMBER(7,2) CHECK (price > 0),
    ups     VARCHAR2(150),
    CONSTRAINT pk_book_store PRIMARY KEY (ups));

-- Table log to store record of every insert on the book store 

create table  book_store_insert_log (
    log_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    insert_time TIMESTAMP,
    rows_inserted NUMBER(7),
    operator VARCHAR2(50));

    
    create table book_store_errors (
    error_id       NUMBER GENERATED ALWAYS AS IDENTITY,
    title          VARCHAR2(4000),
    genre          VARCHAR2(100),
    ratings        VARCHAR2(15),
    price          NUMBER(7,2),
    ups            VARCHAR2(150),
    error_message  VARCHAR2(4000),
    error_date     DATE DEFAULT SYSDATE);








