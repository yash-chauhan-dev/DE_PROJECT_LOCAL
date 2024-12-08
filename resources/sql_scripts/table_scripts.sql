CREATE TABLE product_staging_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255),
    file_location VARCHAR(255),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    status VARCHAR(1)
);