import sqlite3
import pandas as pd

# Connects to an existing database file in the current directory
# If the file does not exist, it creates it in the current directory

db_connect = sqlite3.connect('project.db')

# Instantiate cursor object for executing queries
cursor = db_connect.cursor()

# Method to print query
def printQuery():
    # Extract column names from cursor
    column_names = [row[0] for row in cursor.description]

    # Fetch data and load into a pandas dataframe

    table_data = cursor.fetchall()
    df = pd.DataFrame(table_data, columns=column_names)

    # Examine dataframe
    print(df)
    print("\n\n")
    #print(df.columns)
    
    # Example to extract a specific column
    # print(df['name'])


# Method to create tables
def createTables():
    query = """
        CREATE TABLE Clinic (
            clinicNo int NOT NULL PRIMARY KEY check (clinicNo between 0 and 9999),
            cName varchar(255),
            cAddress varchar(255),
            cTelNo int check (cTelNo between 0 and 9999999999)
        );
    """
    cursor.execute(query)

    query = """
        CREATE TABLE Staff (
            staffNo int NOT NULL PRIMARY KEY check (staffNo between 0 and 9999),
            sName varchar(255),
            sAddress varchar(255),
            sTelNo int check (sTelNo between 0 and 9999999999),
            sDOB DATETIME,
            sPosition varchar(255),
            sSalary int check (sSalary between 0 and 9999999),
            clinicNo int check (clinicNo between 0 and 9999),
            clinicManaged int check (clinicManaged between 0 and 9999),
            CONSTRAINT fk_staff
                FOREIGN KEY (clinicNo) REFERENCES Clinic(clinicNo)
                ON DELETE SET NULL
                FOREIGN KEY (clinicManaged) REFERENCES Clinic(clinicNo)
                ON DELETE SET NULL
        );
    """
    cursor.execute(query)

    query = """
        CREATE TABLE Owner (
            ownerNo int NOT NULL PRIMARY KEY check (ownerNo between 0 and 9999),
            oName varchar(255),
            oAddress varchar(255),
            oTelNo int check (oTelNo between 0 and 9999999999),
            clinicNo int check (clinicNo between 0 and 9999),
            CONSTRAINT fk_owner
                FOREIGN KEY (clinicNo) REFERENCES Clinic(clinicNo)
                ON DELETE SET NULL
        );
    """
    cursor.execute(query)

    query = """
        CREATE TABLE Pet (
            petNo int NOT NULL PRIMARY KEY check (petNo between 0 and 9999),
            pName varchar(255),
            pDOB DATETIME,
            pSpecies varchar(255),
            pBreed varchar(255),
            pColor varchar(255),
            ownerNo int check (ownerNo between 0 and 9999),
            clinicNo int check (clinicNo between 0 and 9999),
            CONSTRAINT fk_pet
                FOREIGN KEY (clinicNo) REFERENCES Clinic(clinicNo)
                ON DELETE SET NULL,
                FOREIGN KEY (ownerNo) REFERENCES Owner(ownerNo)
                ON DELETE SET NULL
        );
    """
    cursor.execute(query)

    query = """
        CREATE TABLE Exam (
            examNo int NOT NULL PRIMARY KEY check (examNo between 0 and 9999),
            eComplaint varchar(255),
            eDescription varchar(255),
            eDate DATETIME,
            eActionTaken varchar(255),
            staffNo int check (staffNo between 0 and 9999),
            petNo int check (petNo between 0 and 9999),
            CONSTRAINT fk_exam
                FOREIGN KEY (staffNo) REFERENCES Staff(staffNo)
                ON DELETE SET NULL,
                FOREIGN KEY (petNo) REFERENCES Pet(petNo)
                ON DELETE SET NULL
        );
        """
    cursor.execute(query)

# Method to insert rows into table
def insertRows():
    query = """
        INSERT INTO Clinic VALUES (0001, 'Caring Claws', '101 Coral Street', 7868530001);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Clinic VALUES (0002, 'Equine', '300 North Avenue', 7838530002);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Clinic VALUES (0003, 'Animal House', '810 South Avenue', 7868530003);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Clinic VALUES (0004, 'Fur Friends', '200 Ocean Street', 7868530004);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Clinic VALUES (0005, 'Marine Animal Care', '410 May Street', 7868530005);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Staff VALUES (0001, 'Alan Brown', '100 Coral Street', 7868530006, '1980-12-10', 'Secretary', 60000, 0001, 0001);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Staff VALUES (0002, 'Anna Black', '320 West Boulevard', 7868530007, '1985-10-05', 'Assistant', 70000, 0002, NULL);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Staff VALUES (0003, 'Sam White', '450 North Avenue', 7868530008, '1970-02-07', 'Veterinary', 100000, 0003, NULL);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Staff VALUES (0004, 'Luisa Red', '300 South Avenue', 7868530009, '1989-11-03', 'Assisant', 75000, 0004, NULL);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Staff VALUES (0005, 'Max Gray', '201 Ocean Street', 7868530010, '1964-03-11', 'Veterinary', 120000, 0005, 0005);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Owner VALUES (0001, 'Philip Brown', '130 Coral Street', 7868530011, 0001);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Owner VALUES (0002, 'Melissa Black', '330 West Boulevard', 7868530012, 0002);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Owner VALUES (0003, 'Noel White', '200 North Avenue', 7868530013, 0003);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Owner VALUES (0004, 'Monica Red', '100 South Avenue', 7868530014, 0004);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Owner VALUES (0005, 'John Gray', '221 Ocean Street', 7868530015, 0004);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Pet VALUES (0001, 'Lulu', '2020-12-10', 'Dog', 'German Sheperd', 'Black', 0001, 0001);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Pet VALUES (0002, 'Hollie', '2015-10-05', 'Cat', 'Siamese', 'Grey', 0002, 0002);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Pet VALUES (0003, 'Kai', '2018-02-07', 'Dog', 'Bulldog', 'White, Black', 0004, 0004);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Pet VALUES (0004, 'Luna', '2019-11-03', 'Cat', 'Siamese', 'Grey', 0003, 0003);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Pet VALUES (0005, 'Yuki', '2028-03-11', 'Horse', 'Mustang', 'White, Brown', 0005, 0005);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Exam VALUES (0001, 'Swollen paw', 'Left paw is swollen', '2022-02-10', 'Antibiotics prescribed', 0001, 0001);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Exam VALUES (0002, 'Not eating', 'Has not eaten in two days', '2022-10-05', 'Soft food recommended', 0002, 0002);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Exam VALUES (0003, 'Throwing up', 'Has been throwing up for three days', '2022-02-07', 'Soft food recommended', 0003, 0003);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Exam VALUES (0004, 'Pregnant', 'Three weeks pregnant', '2022-11-03', 'Examined stomach', 0004, 0004);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Exam VALUES (0005, 'Itchy', 'Biting and itching skin', '2022-03-11', 'Checked for ticks', 0005, 0005);
        """
    cursor.execute(query)

# Method to execute queries
def executeQueries():

    print('\n\nTables after they have been populated with data\n\n')
    query = """
        SELECT *
        FROM Clinic
        ;
        """
    cursor.execute(query)
    printQuery()
    query = """
        SELECT *
        FROM Staff
        ;
        """
    cursor.execute(query)
    printQuery()
    query = """
        SELECT *
        FROM Owner
        ;
        """
    cursor.execute(query)
    printQuery()
    query = """
        SELECT *
        FROM Pet
        ;
        """
    cursor.execute(query)
    printQuery()
    query = """
        SELECT *
        FROM Exam
        ;
        """
    cursor.execute(query)
    printQuery()


    print('\n Output from five queries: \n')
    print('1. List all staff that works in the clinic Caring Claws\n')
    query = """
        SELECT s.staffNo, s.sName
        FROM Staff s, Clinic c
        WHERE s.clinicNo = c.clinicNo AND c.cName LIKE 'Caring Claws';
        """
    cursor.execute(query)
    printQuery()

    print('2. How many dog pets are registered at clinic no 4?\n')
    query = """
        SELECT COUNT(PetNo)
        FROM Pet 
        WHERE clinicNo = 4 AND pSpecies LIKE 'Dog';
        """
    cursor.execute(query)
    printQuery()

    print('3. List the clinic that performed examNo 004\n')
    query = """
        SELECT e.examNo, c.cName, c.clinicNo
        FROM Clinic c, Staff s, Exam e
        WHERE e.staffNo = s.StaffNo AND s.clinicNo = c.clinicNo AND e.examNo = 0004;
        """
    cursor.execute(query)
    printQuery()

    print('4. What is the maximum salary in the Animal House clinic?\n')
    query = """
        SELECT MAX(s.sSalary)
        FROM Staff s, Clinic c
        WHERE c.clinicNo = s.clinicNo AND c.cName LIKE 'Animal House';
        """
    cursor.execute(query)
    printQuery()

    print('5. Close clinicNo 0005 and move all staff, pets and owners to clinicNo 0001\n')
    query = """
        UPDATE Owner
        SET clinicNo = 0001 WHERE clinicNo = 0005; 
        """
    cursor.execute(query)
    query = """
        UPDATE Pet
        SET clinicNo = 0001 WHERE clinicNo = 0005; 
        """
    cursor.execute(query)
    query = """
        UPDATE Staff
        SET clinicNo = 0001 WHERE clinicNo = 0005; 
        """
    cursor.execute(query)
    query = """
        DELETE FROM Clinic WHERE clinicNo = 0005; 
        """
    cursor.execute(query)
    query = """
        SELECT *
        FROM Clinic; 
        """
    cursor.execute(query)
    printQuery()

createTables()
insertRows()
executeQueries()

# Commit any changes to the database
db_connect.commit()

# Close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
db_connect.close()
