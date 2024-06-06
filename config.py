import pyodbc
import tableauserverclient as TSC
from sqlalchemy import create_engine

def sigin_tb_setup(site_name):
    # ฟังก์ชันนี้ใช้สำหรับการเข้าสู่ระบบ Tableau Server
    server = TSC.Server('http://172.29.32.103/', use_server_version=True)
    # ใช้ Personal Access Token เพื่อเข้าสู่ระบบ Tableau Server
    tableau_auth = TSC.PersonalAccessTokenAuth('ts', 'ZVJBR8lwRbCOg26kCFZMoQ==:dyyesDTlIvvqDCDvY2JLYVhBuE4Wizct', site_name)  # โทเค็นของผู้ใช้ supanut.sri
    return server, tableau_auth

def singin_to_postgresql_setup():
    # ฟังก์ชันนี้ใช้สำหรับการเชื่อมต่อกับ PostgreSQL
    try:
        engine = create_engine('postgresql://readonly:coreP%40ssw0rd@172.29.32.102:8060/workgroup')
    except Exception as e:
        # แสดงข้อความข้อผิดพลาดหากการเชื่อมต่อล้มเหลว
        print(e)

    return engine

def singin_to_panda_setup():
    # ฟังก์ชันนี้ใช้สำหรับการเชื่อมต่อกับฐานข้อมูล SQL Server
    conn = pyodbc.connect("Driver=SQL Server;"  # ใช้ไดรเวอร์ SQL Server
                          "Server=SIA09-ICPR01;"  # ชื่อเซิร์ฟเวอร์
                          "Database=SiIMC_MGHT;"  # ชื่อฐานข้อมูล
                          "uid=UserPandaITF;pwd=P@ssw0rd@ssis")  # ชื่อผู้ใช้และรหัสผ่าน
                      
    return conn

def signin_to_pg_credent():
    # ฟังก์ชันนี้ใช้สำหรับการเชื่อมต่อกับ PostgreSQL ด้วยข้อมูลประจำตัวอื่น
    try:
        engine = create_engine('postgresql://sidata:P%40ssw0rd@172.30.224.195:5432/analyst')
    except Exception as e:
        # แสดงข้อความข้อผิดพลาดหากการเชื่อมต่อล้มเหลว
        print(e)

    return engine

def singin_to_lake_dev_setup():
    # ฟังก์ชันนี้ใช้สำหรับการเชื่อมต่อกับฐานข้อมูล SQL Server ในสภาพแวดล้อมการพัฒนา
    conn = pyodbc.connect("Driver=SQL Server;"  # ใช้ไดรเวอร์ SQL Server
                          "Server=sia09-icdvx1.sihmis.si;"  # ชื่อเซิร์ฟเวอร์ในสภาพแวดล้อมการพัฒนา
                          "Database=StgPanda;"  # ชื่อฐานข้อมูล
                          "uid=UserDevPandaITF;pwd=P@ssword@ssis")  # ชื่อผู้ใช้และรหัสผ่าน
                      
    return conn
