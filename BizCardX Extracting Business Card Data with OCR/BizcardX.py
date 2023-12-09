import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import easyocr
from easyocr import Reader
import mysql.connector as sql 
from PIL import Image
import cv2
import os
import matplotlib.pyplot as plt
import re

#Creating function to save image
def save_card(uploaded_card):
        save_path = os.path.join(os.getcwd(), "uploaded_cards")
        os.makedirs(save_path, exist_ok=True)  # Create the directory if it doesn't exist
        saved_img = os.path.join(save_path, uploaded_card.name)

        with open(saved_img, "wb") as f:
            f.write(uploaded_card.getbuffer())

        return saved_img

#Initializing the EASYOCR Reader
read = easyocr.Reader(['en'])

#Connecting the SQL Database
mydb = sql.connect(host="localhost",
                   user="root",
                   password="",
                   #database = "bizcardx_db",
                  )
mycursor = mydb.cursor(buffered=True)

#Creating database (if not already) and using the same
mycursor.execute("CREATE DATABASE IF NOT EXISTS bizcardx_db")
mycursor.execute("USE bizcardx_db")

#Creating table 
mycursor.execute("""CREATE TABLE IF NOT EXISTS biz_card_data
                 (id INTEGER PRIMARY KEY AUTO_INCREMENT,
                 company_name TEXT,
                 card_holder TEXT,
                 designation TEXT,
                 mobile_number VARCHAR(50),
                 email_id TEXT,
                 website TEXT,
                 area TEXT,
                 city TEXT,
                 state TEXT,
                 pin_code VARCHAR(10),
                 image LONGBLOB
                 )""")

#Setting page config.
st.set_page_config (page_title = "BizCardx: Extracting Data From Business Card with OCR | By Rohit Atul Kunte.",
                    layout = "wide",
                    initial_sidebar_state="expanded",
                    menu_items={'About':"""# This app is created by Rohit Atul Kunte!"""})
st.markdown("<h1 style = 'text_align: center; color: white;'> BizCardx: Extracting Data From Business Card with OCR</h1>",  unsafe_allow_html =True)

#Setting up bg image
def setting_bg():
    st.markdown(f"""
        <style> 
                .stApp {{
                background-image: url("https://wallpapercave.com/wp/wp3562621.jpg");
                background-size : cover
                }}
                </style>""",unsafe_allow_html=True
                )
setting_bg()

#Creating options menu:
selected = option_menu(None,["Home","Upload & Extract","Alter or Update Card details"],
                       icons = ["home","cloud-upload-alt","edit"],
                       default_index=0,
                       orientation="horizontal",
                       styles={"nav-link":{"font-size":"25px","text_align":"centre","margin":"0px","--hover-color":"#AB63FA","transition":"color 0.3s ease, background-color 0.3s ease"},
                               "icon":{"font-size":"25px"},
                               "container":{"max-width":"6000px","padding":"10px","border-radius":"5px"},
                               "nav-link-selected":{"background-color":"#AB63FA","color":"white"}
                               })

#Home menu:
if selected == "Home":
    col1,col2 = st.columns(2)
    with col1:
        st.markdown('## :blue [**Technologies used :**] Python, easy OCR, Streamlit, SQL, Pandas')
        st.markdown('## :blue [**Overview:**] In this streamlit web app you can upload an image of a business card and extract relevant information from it using easyOCR. You can view, modify or delete the extracted data in this app. This app would also allow users to save the extracted information into a database along with the uploaded business card image. The database would be able to store multiple entries, each with its own business card image and extracted information.")')

#Upload & Extract Menu
if selected == "Upload & Extract":
    st.markdown("### Upload a Business Card")
    uploaded_card = st.file_uploader("Upload here",label_visibility="collapsed",type=["png","jpeg","jpg"],accept_multiple_files=False)  
    
    if uploaded_card is not None:
        saved_img = save_card(uploaded_card)

        def image_preview(saved_img,res):
            for (bbox,text,prob) in res:
                (tl,tr,br,bl) = bbox
                tl = (int(tl[0]),int(tl[1]))
                tr = (int(tr[0]),int(tr[1]))
                br = (int(br[0]),int(br[1]))
                bl = (int(bl[0]),int(bl[1]))
                cv2.rectangle(saved_img,tl,br,(0,255,0),2)
                cv2.putText(saved_img, text, (tl[0], tl[1] - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 0, 0), 2)
            plt.rcParams['figure.figsize']=(15,15)
            plt.axis('off')
            plt.imshow(saved_img)

        #Displaying the uploaded card
        col1, col2 = st.columns(2, gap="large")
        with col1:
            st.markdown("#     ")
            st.markdown("#     ")
            st.markdown("### Card upload is successful.")
            st.image(uploaded_card)
        with col2:
            st.markdown("#    ")
            st.markdown("#    ")
            with st.spinner("Processing Image..."):
                st.set_option('deprecation.showPyplotGlobalUse', False)
                image = cv2.imread(saved_img)
                res = read.readtext(image)
                st.markdown("### Image Processed and Data Extracted")
                st.pyplot(image_preview(image, res))

        # easy ocr
        saved_img = os.path.join(os.getcwd(), "uploaded_cards", uploaded_card.name)
        result = read.readtext(saved_img, detail=0, paragraph=False)

        # Function to convert image to binary
        def img_to_binary(file):
            with open(file, 'rb') as file:
                binaryData = file.read()
            return binaryData
        
        def identify_and_format(raw_data):
            formatted_data = {
                "company_name": "empty",
                "card_holder": "empty",
                "designation": "empty",
                "mobile_number": "empty",
                "email_id": "empty",
                "website": "empty",
                "area": "empty",
                "city": "empty",
                "state": "empty",
                "pin_code": "empty",
                "image" : str(img_to_binary(saved_img)),
            }
            
            for ind, i in enumerate(raw_data):
                # Check for Website URL
                if "www" in i.lower():
                    formatted_data["website"] = i
                elif ind > 0 and "www" in raw_data[ind - 1].lower():
                    formatted_data["website"] = raw_data[ind - 1] + "." + i
                    
                # Check for Email ID
                elif "@" in i:
                    formatted_data["email_id"] = i

                # Check for Mobile Number
                elif "-" in i:
                    if "mobile_number" not in formatted_data or formatted_data["mobile_number"] == "empty":
                        formatted_data["mobile_number"] = i
                    else:
                        formatted_data["mobile_number"] += f" & {i}"
                        
                # Check for Company Name
                elif ind == len(raw_data)-1:
                    formatted_data["company_name"] = i

                # Check for Card Holder Name
                elif ind == 0:
                    formatted_data["card_holder"] = i
                
                # Check for Designation
                elif ind == 1:
                    formatted_data["designation"] = i
                
                # Check for Area
                elif re.findall("^[0-9].+,[a-zA-Z]+", i):
                    formatted_data["area"] = i.split(',')[0]
                elif re.findall('[0-9] [a-zA-Z]+', i):
                    formatted_data["area"] = i
                
                # Check for City Name
                match1 = re.findall('.+St,([a-zA-Z]+).+', i)
                match2 = re.findall('.+St,([a-zA-Z]+).+', i)
                match3 = re.findall('^[E].*', i)
                if match1:
                    formatted_data["city"] = match1[0]
                elif match2:
                    formatted_data["city"] = match2[0]
                elif match3:
                    formatted_data["city"] = match3[0]

                # Check for State
                state_match = re.findall("[a-zA-Z]{9}+[0-9]", i)
                if state_match:
                    formatted_data["state"] = i[:9]
                elif re.findall ('^[0-9].+, ([a-zA-Z]+);', i):
                    formatted_data["state"] = i.split()[-1]
                
                # Check for Pincode
                if len(i) <= 9 and i.isdigit():
                    formatted_data["pin_code"] = i
                elif re.findall('[a-zA-Z]{9}+[0-9]', i): 
                    formatted_data["pin_code"] = i[5:]

            for i in formatted_data:
                formatted_data[i] = [formatted_data[i]]
            return formatted_data

        data = identify_and_format(result)
        print (data)

        # Creating dataframe
        def create_df(data):
            df = pd.DataFrame(data)
            return df
        df = create_df(data)
        st.success('### Data has been successfully extracted!')
        st.write(df)
        print (df)
        
        if st.button("Upload to Database"):
            for i,row in df.iterrows():
                sql = """INSERT INTO biz_card_data(company_name,card_holder,designation,mobile_number,email_id,website,area,city,state,pin_code,image)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""   

                mycursor.execute(sql,tuple(row))
                mydb.commit()
            st.success('#### Data uploading to database is a success!')

#Modify Menu
if selected == "Alter or Update Card details":
    col1,col2,col3 = st.columns([3,3,2])
    col2.markdown ('## Alter or Delete the data present here.')
    column1,column2 = st.columns(2,gap='large')
    try:
        with column1:
            mycursor.execute("SELECT id FROM biz_card_data")
            result = mycursor.fetchall()
            print(result)
            business_card ={}
            for row in result:
                business_card[row[0]]=row[0]
            selected_card = st.selectbox("Select a id to update",list(business_card.keys()))
            st.markdown('### update or modify any data below')
            mycursor.execute("SELECT company_name,card_holder,designation,mobile_number,email_id,website,area,city,state,pin_code from biz_card_data WHERE id=%s",
                             (selected_card,))
            result = mycursor.fetchone()

            #Displaying all the info. 
            company_name = st.text_input("Company_Name",result[0])
            card_holder = st.text_input("Card_Holder",result[1])
            designation = st.text_input("Designation",result[2])
            mobile_number = st.text_input("Mobile_Number",result[3])
            email_id = st.text_input("Email_Id",result[4])
            website = st.text_input("Website",result[5])
            area = st.text_input("Area",result[6])
            city = st.text_input("City",result[7])
            state = st.text_input("State",result[8])
            pin_code = st.text_input("Pin_Code",result[9])

            if st.button ("Update the changes"):
                mycursor.execute("""UPDATE biz_card_data SET company_name=%s,card_holder=%s,designation=%s,mobile_number=%s,email_id=%s,website=%s,area=%s,city=%s,state=%s,pin_code=%s
                                    WHERE id=%s""", (company_name,card_holder,designation,mobile_number,email_id,website,area,city,state,pin_code,selected_card))
                mydb.commit()
                st.success ("Information has been updated successfully")
        
        with column2:
            mycursor.execute("SELECT id FROM biz_card_data")
            result = mycursor.fetchall()
            business_cards = {}
            for row in result:
                business_cards[row[0]] = row[0]
            selected_card = st.selectbox("Select a id to Delete", list(business_cards.keys()))
            st.write(f"### You have selected :green[**{selected_card}'s**] card to delete")
            st.write("#### Proceed to delete this card?")

            if st.button("Yes Delete Business Card"):
                mycursor.execute(f"DELETE FROM biz_card_data WHERE id='{selected_card}'")
                mydb.commit()
                st.success("Business card information deleted from database.")
    except:
        st.warning("There is no data available in the database")
    
    if st.button("View updated data"):
        mycursor.execute("select company_name,card_holder,designation,mobile_number,email_id,website,area,city,state,pin_code from biz_card_data")
        updated_df = pd.DataFrame(mycursor.fetchall(),columns=["Company_Name","Card_Holder","Designation","Mobile_Number","Email_Id","Website","Area","City","State","Pin_Code"])
        st.write(updated_df)