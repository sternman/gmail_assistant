import streamlit as st
import gmail
import pandas as pd
from datetime import datetime, timedelta
import os, sys
import duckdb as D
from multiprocessing.pool import ThreadPool

st.set_page_config(
        page_title="Gmail Client", page_icon=":email:", layout="wide"
    )

#google auth - need to make this a class
service = gmail.gmail_authenticate()
r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns((6, 15, 10, 10, 4))

r2c1, r2c2 = st.columns(( 40,10  ))

r3c1, r3c2, r3c3, r3c4, r3c5 = st.columns((6, 15, 15, 35, 4))
r4c1, r4c2, r4c3 = st.columns((1, 60, 20))

def month_number(month_name):
    month_name = month_name.lower()
    
    if month_name == "jan":
        return 1
    elif month_name == "feb":
        return 2
    elif month_name == "mar":
        return 3
    elif month_name == "apr":
        return 4
    elif month_name == "may":
        return 5
    elif month_name == "jun":
        return 6
    elif month_name == "jul":
        return 7
    elif month_name == "aug":
        return 8
    elif month_name == "sep":
        return 9
    elif month_name == "oct":
        return 10
    elif month_name == "nov":
        return 11
    elif month_name == "dec":
        return 12

emls = []
def get_details(msg):
    global emls
    
    # for i, m in enumerate(msgs):
        
    payload, headers, parts, froms, tos, subjects, dates = gmail.read_message(service, msg)
    if len(froms) == 1:


        try:
            if len(gmail.list_text) > 0:
                
                arr= []
                for lt in gmail.list_text:
                    if isinstance(lt, bytes) :
                        txt = str(lt, 'utf-8')
                    else:
                        txt = lt
                    # print(txt)
                    arr.append(txt)
                gmail.list_text = arr
                text = ",".join(arr)
            else:
                text = ""
            files = gmail.list_filename
            files = [x for x in files if "" not in x]
            filename = ",".join(list(set(files)))
            if len(tos) == 0:
                tos = ""
            else:
                tos = tos[0]

            if len(gmail.list_file_size) == 0:
                file_size = 0
            else:
                file_size = int(gmail.list_file_size[0])

            if len(dates) == 1:
                dates = dates[0]
                dd = dates.split(" ")
           
                month, day, hour, minute, second, tz_offset = "", "", "", "", "", ""
                
                if dd[1] == "":
                    tt = dd[5].split(":")
                    month = month_number(dd[3])
                    tz_offset = int(dd[6])
                    day = int(dd[2])
                    year = int(dd[4])
                else:
                    tt = dd[4].split(":")
                    month = month_number(dd[2])
                    tz_offset = int(dd[5])
                    day = int(dd[1])
                    year = int(dd[3])
                
                tz_offset = tz_offset / 100
                hour = int(tt[0])
                minute = int(tt[1])
                second = int(tt[2])
                
                dt = datetime(year, month, day, hour, minute, second )
                dt = dt + timedelta(hours=tz_offset)
                # dates = datetime.strptime(dates[0], "%a, %w %b %Y %H:%M:%S %z (%Z)")
                mime_type = ""
                if len(gmail.list_mime_type) > 0:
                    mime_type = gmail.list_mime_type[0]
                
                    
            if len(subjects) == 0:
                subjects = ""
            else:
                subjects = subjects[0]

            attach_size = 0
            if len(gmail.list_attachment_sizes) > 0:
                attach_size = sum(gmail.list_attachment_sizes)
            attach_count = 0
            if len(gmail.list_attachments) > 0:
                attach_count = len(gmail.list_attachments)
            eml = (
                dt,
                froms[0], 
                tos, 
                subjects, 
                file_size,               
                text, 
                mime_type,
                filename,
                attach_size,
                attach_count
                )
            emls.append(eml)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(f"====={e} : {exc_tb.tb_lineno} --> {dates} --> {dd}")
            

    
    # r3c2.write(emls)
def get_custom_details_batch(msgs, custom_pattern):
    #print(processList)
    pool = ThreadPool(1)
    results = pool.map(get_details, msgs)
    df = pd.DataFrame(emls)
    df = df.rename(columns={0:"Date", 
                            1:"Email_From", 
                            2:"Email_To", 
                            3:"Subject", 
                            4:"Size",
                            5:"Body",
                            6:"Mime Type",
                            7:"File Name",
                            8:"Attachment Sizes",
                            9:"Attachment Count"
                            })
    r3c1.write(f"Found {len(msgs)} emails")
    r3c2.write(f"{custom_pattern}")
    r4c2.write(df)
    sizes = D.query("""
                            select sum(cast("Attachment Sizes" as int)) attach_sizes, sum(cast(size as int)) email_sizes
                            from df""").to_df()
    
    attachment_sizes = round(sizes["attach_sizes"].to_list()[0] / 1024.0 / 1024.0, 2)
    email_sizes = round(sizes["email_sizes"].to_list()[0] / 1024.0 / 1024.0, 2)

    
    r3c4.write(f"Total Size: {round(attachment_sizes + email_sizes, 2)} MB. Attachments: {attachment_sizes} MB Emails: {email_sizes} MB")
    senders = D.query("select Email_From, count(1) AS Cnt from df group by all").to_df()
    r4c3.write(senders)
    
def get_details_batch(msgs, criteria, pattern):
    #print(processList)
    pool = ThreadPool(1)
    results = pool.map(get_details, msgs)
    df = pd.DataFrame(emls)
    df = df.rename(columns={0:"Date", 
                            1:"Email_From", 
                            2:"Email_To", 
                            3:"Subject", 
                            4:"Size",
                            5:"Body",
                            6:"Mime Type",
                            7:"File Name",
                            8:"Attachment Sizes",
                            9:"Attachment Count"
                            })
    r3c1.write(f"Found {len(msgs)} emails")
    r3c2.write(f"{criteria} : {pattern}")
    r4c2.write(df)
    sizes = D.query("""
                            select sum(cast("Attachment Sizes" as int)) attach_sizes, sum(cast(size as int)) email_sizes
                            from df""").to_df()
    
    attachment_sizes = round(sizes["attach_sizes"].to_list()[0] / 1024.0 / 1024.0, 2)
    email_sizes = round(sizes["email_sizes"].to_list()[0] / 1024.0 / 1024.0, 2)

    
    r3c4.write(f"Total Size: {round(attachment_sizes + email_sizes, 2)} MB. Attachments: {attachment_sizes} MB Emails: {email_sizes} MB")
    senders = D.query("select Email_From, count(1) Cnt from df group by all").to_df()
    r4c3.write(senders)

def search(criteria, pattern, date_filter, filter_date):
    print(f"{criteria} : {pattern} AND {date_filter}:{filter_date}")

    msgs = gmail.search_messages(service, f"{criteria}:{pattern} AND {date_filter}:{filter_date}")
    print(f"Found: {len(msgs)} emails")

    
    r3c1.write(f"Found {len(msgs)} emails")
    r3c2.write(f"{criteria} : {pattern}")
    r3c3.button("Get Size Info (slow!)", on_click=get_details_batch, args=([msgs, criteria, pattern]))
    # df = pd.DataFrame(emls)
    # st.write(df)

def custom_search(custom_pattern):
    st.write("custom search")

    msgs = gmail.search_messages(service, f"{custom_pattern}")
    print(f"Found: {len(msgs)} emails")

    
    r3c1.write(f"Found {len(msgs)} emails")
    r3c2.write(f"{criteria} : {pattern}")
    r3c3.button("Get Size Info (slow!)", on_click=get_custom_details_batch, args=([msgs, custom_pattern]))

    
criteria = r1c1.selectbox("Pick search criteria", ["from", "to", "subject"])
pattern = r1c2.text_input("Search pattern", value="duane.lee@midnightinsights.com") 

date_filter = r1c3.selectbox("Date Filters", ["before","after"])
filter_date = r1c4.date_input("Enter date")
r1c5.write("")
r1c5.write("")
r1c5.button("Search", on_click=search, args=([criteria, pattern, date_filter, filter_date]))


custom_pattern = r2c1.text_input("Custom Search", value="from:anne.frank.ps@yrdsb.ca AND after:2023-09-01")

r2c2.write("")
r2c2.write("")
r2c2.button("Custom Search", on_click=custom_search, args=([custom_pattern]) )































