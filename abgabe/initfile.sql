SET GLOBAL sql_mode = 'ANSI_QUOTES';
USE kappa-view;
    

CREATE TABLE British_Online_Retail (
      offset INT NOT NULL,
      InvoiceNo VARCHAR(30) NOT NULL,
      StockCode VARCHAR(30) NOT NULL,
      Description VARCHAR(1000),
      Quantity INT,
      InvoiceDate DATETIME,
      UnitPrice DECIMAL(10,2),
      CustomerID VARCHAR(30),
      prediction INT,
      PRIMARY KEY (offset)
);

CREATE TABLE Other_Online_Retail (
      offset INT NOT NULL,
      InvoiceNo VARCHAR(30) NOT NULL,
      StockCode VARCHAR(30) NOT NULL,
      Description VARCHAR(1000),
      Quantity INT,
      InvoiceDate DATETIME,
      UnitPrice DECIMAL(10,2),
      CustomerID VARCHAR(30),
      Country VARCHAR(1000),
      PRIMARY KEY (offset)
);
