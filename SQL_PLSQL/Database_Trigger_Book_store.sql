
--Creating Trigger to automate converting string ratings to number and to count number of inserted rows

Create or replace trigger book_store_trgg
before insert 
on book_store
for each row
begin
    case :NEW.ratings   when 'Zero'     then    :NEW.ratings := '0';
                        when 'One'      then    :NEW.ratings := '1';
                        when 'Two'      then    :NEW.ratings := '2';
                        when 'Three'    THEN    :NEW.ratings := '3';
                        when 'Four'     then    :NEW.ratings := '4';
                        when 'Five'     then    :NEW.ratings := '5';
                        when 'Six'      then    :NEW.ratings := '6';
                        when 'Seven'    then    :NEW.ratings := '7';
                        else                    :NEW.ratings := '8';
    END CASE;
end;




CREATE OR REPLACE TRIGGER book_store_log_trg
FOR INSERT ON book_store
COMPOUND TRIGGER

-- Note this variables can be shared with the compound tigger unlike the single trigger that needs a package varible
    rows_in_current_insert NUMBER := 0;
    total_rows_in_tbl NUMBER := 0;
    
    BEFORE STATEMENT IS
    BEGIN
        NULL;
    END BEFORE STATEMENT;

    AFTER STATEMENT IS
    BEGIN
        SELECT COUNT(*) INTO total_rows_in_tbl FROM BOOK_STORE;
        
        INSERT INTO book_store_insert_log (
            insert_time,
            rows_inserted,
            operator)
        VALUES (
            SYSTIMESTAMP,
            total_rows_in_tbl,
            user);
    END AFTER STATEMENT;

END book_store_log_trg;