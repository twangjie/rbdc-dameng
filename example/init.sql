CREATE TABLE Biz_Activity (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    pc_link VARCHAR(255),
    h5_link VARCHAR(255),
    pc_banner_img VARCHAR(255),
    h5_banner_img VARCHAR(255),
    sort VARCHAR(255),
    status INT,
    remark TEXT,
    create_time DATETIME,
    version BIGINT,
    delete_flag INT
);