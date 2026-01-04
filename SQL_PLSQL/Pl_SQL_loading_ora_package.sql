-- Creating procedure to bulk insert book data from API data extraction
-- Due to bulk insert method. it is imperative to create a collection which will store the row data to be bulk inserted 

CREATE OR REPLACE TYPE book_data_rec_type AS OBJECT (
    title  VARCHAR2(4000),
    genre  VARCHAR2(75),
    ratings  VARCHAR2(25), 
    price  NUMBER(7,2),
    ups VARCHAR2(50));
CREATE OR REPLACE TYPE book_data_tbl_type AS TABLE OF book_data_rec_type;


-- Creating a package that store the procedure for bulk insert of book data from API extraction
-- Creating Package Header
CREATE OR REPLACE PACKAGE Books_pkg AS
    PROCEDURE bulk_upsert_books ( p_books  IN  book_data_tbl_type,p_failed_rows OUT book_data_tbl_type);
    PROCEDURE get_error_log(p_errors OUT book_data_tbl_type);
    PROCEDURE clear_error_log;
    PROCEDURE retry_failed_rows(p_retry_failed_rows OUT book_data_tbl_type);
END Books_pkg;

-- Creating package Books_pkg Body or package implementation
CREATE OR REPLACE PACKAGE BODY Books_pkg AS
    PROCEDURE bulk_upsert_books (p_books IN  book_data_tbl_type,p_failed_rows OUT book_data_tbl_type)
    IS
-- Temp arrays for indexes of failed rows
        error_mes VARCHAR2(2500);
        failed_idx  SYS.ODCINUMBERLIST := SYS.ODCINUMBERLIST();

        TYPE title_tab   IS TABLE OF book_store.title%TYPE;
        TYPE genre_tab   IS TABLE OF book_store.genre%TYPE;
        TYPE ratings_tab IS TABLE OF book_store.ratings%TYPE;
        TYPE price_tab   IS TABLE OF book_store.price%TYPE;
        TYPE ups_tab     IS TABLE OF book_store.ups%TYPE;

        l_titles   title_tab := title_tab();
        l_genres   genre_tab := genre_tab();
        l_ratings  ratings_tab := ratings_tab();
        l_prices   price_tab := price_tab();
        l_ups      ups_tab := ups_tab();

    BEGIN
-- Initialize output
        p_failed_rows := book_data_tbl_type();
        error_mes := 'holder';
        FOR i IN 1 .. p_books.COUNT LOOP
        l_titles.EXTEND;   l_titles(i)  := p_books(i).title;
        l_genres.EXTEND;   l_genres(i)  := p_books(i).genre;
        l_ratings.EXTEND;  l_ratings(i) := p_books(i).ratings;
        l_prices.EXTEND;   l_prices(i)  := p_books(i).price;
        l_ups.EXTEND;      l_ups(i)     := p_books(i).ups;
        END LOOP;


-- Bulk merge for insert or update
        FORALL i IN 1 .. l_titles.COUNT SAVE EXCEPTIONS
            MERGE INTO book_store bs
            USING (
                SELECT l_titles(i)   AS title,
                       l_genres(i)   AS genre,
                       l_ratings(i)  AS ratings,
                       l_prices(i)   AS price,
                       l_ups(i)      AS ups
                FROM dual) pb
            ON (bs.ups = pb.ups)
            WHEN MATCHED THEN
                UPDATE SET  bs.title   =    pb.title,
                            bs.genre   =    pb.genre,
                            bs.ratings =    pb.ratings,
                            bs.price   =    pb.price
            WHEN NOT MATCHED THEN
                INSERT (title, genre,ratings,price,ups)
                VALUES (pb.title, pb.genre, pb.ratings, pb.price, pb.ups);
            
        EXCEPTION
        WHEN OTHERS THEN
            

            FOR e IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
                failed_idx.EXTEND;
                failed_idx(failed_idx.COUNT) := SQL%BULK_EXCEPTIONS(e).ERROR_INDEX;
                error_mes := SQLERRM(SQL%BULK_EXCEPTIONS(e).ERROR_CODE);
            END LOOP;

-- Populate failed rows
            FOR k IN 1 .. failed_idx.COUNT LOOP
            INSERT INTO book_store_errors(title, genre, ratings, price, ups,error_message) 
                VALUES(
                    p_books(failed_idx(k)).title,
                    p_books(failed_idx(k)).genre,
                    p_books(failed_idx(k)).ratings,
                    p_books(failed_idx(k)).price,
                    p_books(failed_idx(k)).ups,
                    error_mes);
                
                p_failed_rows.EXTEND;
                p_failed_rows(p_failed_rows.COUNT) := p_books(failed_idx(k));
            END LOOP;
    END bulk_upsert_books;

    
    PROCEDURE get_error_log(p_errors OUT book_data_tbl_type)
    IS
    BEGIN
        p_errors := book_data_tbl_type();

        FOR rec IN (SELECT title, genre, ratings, price, ups FROM book_store_errors ORDER BY error_id)LOOP
            p_errors.EXTEND;
            p_errors(p_errors.COUNT) := book_data_rec_type(rec.title, rec.genre, rec.ratings, rec.price, rec.ups);
        END LOOP;
    END get_error_log;

    
    PROCEDURE clear_error_log
    IS
    BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE book_store_errors';
    END clear_error_log;

    
    PROCEDURE retry_failed_rows(p_retry_failed_rows OUT book_data_tbl_type)
    IS
        l_failed book_data_tbl_type;
    BEGIN
-- First retrieve all previously failed rows
        get_error_log(l_failed);

-- If no failed rows, return empty list
        p_retry_failed_rows := book_data_tbl_type();
        IF l_failed.COUNT = 0 THEN 
            RETURN; 
        END IF;

-- attempt to insert again
        bulk_upsert_books(l_failed, p_retry_failed_rows);

-- clear old log entries if retries successful
        clear_error_log;
    
    
    END retry_failed_rows;
END Books_pkg;