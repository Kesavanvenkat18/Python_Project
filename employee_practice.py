import pandas as pd
import matplotlib.pyplot as plt
import pymysql


connection = pymysql.connect(host='localhost',
                             user='root',
                             password='admin',
                             database='customer_details' ,
                             cursorclass=pymysql.cursors.DictCursor)
with connection.cursor() as cursor:
    create_table = """
    create table if not exists employee_practice(
        Id int primary key auto_increment,
        name varchar(100) not null,
        Employee_id varchar(100) not null,
        StartingSalary int,
        CurrentSalary int,
        Department  varchar(100) not null,
        Address  varchar(100) not null,
        JoiningDate DATETIME NULL,
        MobileNo varchar(100) not null,
        Email  varchar(100) not null,
        ExperienceYears int not null,
        Status varchar(20) not null
    );
                   """
    cursor.execute(create_table)

df = pd.read_csv("employee_practice_data.csv")
df["StartingSalary"]=pd.to_numeric(df["StartingSalary"],errors="coerce")
df["CurrentSalary"]=pd.to_numeric(df["CurrentSalary"], errors="coerce")

df["MobileNo"] = (
    df["MobileNo"]
    .astype(str)
    .str.strip()
    .str.extract(r'([0-9]\d{9})')
)

df["JoiningDate"]=pd.to_datetime(df["JoiningDate"],errors="coerce",dayfirst=True)

"""df["JoiningDate"] = pd.to_datetime(
    df["JoiningDate"],
    format="%d/%m/%Y",
    errors="coerce"
)"""

df[["StartingSalary","CurrentSalary","ExperienceYears"]]=df[["StartingSalary","CurrentSalary","ExperienceYears"]].fillna(0)
df.fillna({
    "Department": "Unknown",
    "Address": "No Address",
    "Email": "No Email",
    "MobileNo": "No MobileNo",
    "Status": "Undefined",
}, inplace=True)
#df["JoiningDate"] = df["JoiningDate"].fillna(pd.Timestamp("1900-01-01"))


df.to_csv("employee_practice_data1.csv",index=False)


insert_query="insert into employee_practice(name,Employee_id,StartingSalary,CurrentSalary,Department,Address,JoiningDate,MobileNo,Email,ExperienceYears,Status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

data = df.values.tolist()

with connection.cursor() as cursor:
    cursor.executemany(insert_query, data)
    connection.commit()

cursor.close()
connection.close()

df["SalaryGrowth"] = df["CurrentSalary"] - df["StartingSalary"]

plt.figure()
plt.hist(df["SalaryGrowth"].dropna(), bins=10)
plt.xlabel("Salary Growth")
plt.ylabel("Number of Employees")
plt.title("Salary Growth Distribution")
plt.savefig('Salary Growth Distribution.jpg')
plt.show()

