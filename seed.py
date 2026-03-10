"""
Database Seed Script - FIXED (no duplicate BSE codes)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import engine, Base, SessionLocal, User, BSEStock
from auth import hash_password

BSE_STOCKS = [
    ("500325","RELIANCE","Reliance Industries Ltd","Energy"),
    ("532540","TCS","Tata Consultancy Services Ltd","IT"),
    ("500180","HDFCBANK","HDFC Bank Ltd","Banking"),
    ("532174","INFY","Infosys Ltd","IT"),
    ("500696","HINDUNILVR","Hindustan Unilever Ltd","FMCG"),
    ("500209","ICICIBANK","ICICI Bank Ltd","Banking"),
    ("500112","SBIN","State Bank of India","Banking"),
    ("532978","BHARTIARTL","Bharti Airtel Ltd","Telecom"),
    ("500228","LT","Larsen and Toubro Ltd","Infrastructure"),
    ("500875","ITC","ITC Ltd","FMCG"),
    ("500034","BAJFINANCE","Bajaj Finance Ltd","Finance"),
    ("500103","KOTAKBANK","Kotak Mahindra Bank Ltd","Banking"),
    ("507685","WIPRO","Wipro Ltd","IT"),
    ("523395","HCLTECH","HCL Technologies Ltd","IT"),
    ("532155","AXISBANK","Axis Bank Ltd","Banking"),
    ("500470","TATASTEEL","Tata Steel Ltd","Metal"),
    ("500480","TATAMOTORS","Tata Motors Ltd","Auto"),
    ("500182","HEROMOTOCO","Hero MotoCorp Ltd","Auto"),
    ("500520","MM","Mahindra and Mahindra Ltd","Auto"),
    ("532648","MARUTI","Maruti Suzuki India Ltd","Auto"),
    ("500410","ASIANPAINT","Asian Paints Ltd","Consumer"),
    ("500790","SUNPHARMA","Sun Pharmaceutical Industries","Pharma"),
    ("500087","CIPLA","Cipla Ltd","Pharma"),
    ("500124","DRREDDY","Dr Reddys Laboratories Ltd","Pharma"),
    ("532281","HDFCLIFE","HDFC Life Insurance Co Ltd","Insurance"),
    ("540777","SBILIFE","SBI Life Insurance Co Ltd","Insurance"),
    ("512599","ADANIPORTS","Adani Ports and SEZ Ltd","Infrastructure"),
    ("542066","ADANIENT","Adani Enterprises Ltd","Conglomerate"),
    ("500002","ABB","ABB India Ltd","Engineering"),
    ("508869","APOLLOTYRE","Apollo Tyres Ltd","Auto Parts"),
    ("500425","APOLLOHOSP","Apollo Hospitals Enterprise Ltd","Healthcare"),
    ("532977","BAJAJAUTO","Bajaj Auto Ltd","Auto"),
    ("500490","BAJAJFINSV","Bajaj Finserv Ltd","Finance"),
    ("532523","BANKBARODA","Bank of Baroda","Banking"),
    ("500493","BHEL","Bharat Heavy Electricals Ltd","Engineering"),
    ("500469","BPCL","Bharat Petroleum Corp Ltd","Energy"),
    ("533229","COALINDIA","Coal India Ltd","Mining"),
    ("500440","CUMMINSIND","Cummins India Ltd","Engineering"),
    ("500096","DABUR","Dabur India Ltd","FMCG"),
    ("532868","DLF","DLF Ltd","Real Estate"),
    ("500300","GRASIM","Grasim Industries Ltd","Cement"),
    ("517354","HAVELLS","Havells India Ltd","Consumer Electricals"),
    ("532187","INDUSINDBK","IndusInd Bank Ltd","Banking"),
    ("530965","IOC","Indian Oil Corp Ltd","Energy"),
    ("532514","IGL","Indraprastha Gas Ltd","Energy"),
    ("500510","JSWSTEEL","JSW Steel Ltd","Metal"),
    ("500257","LUPIN","Lupin Ltd","Pharma"),
    ("517477","MOTHERSON","Motherson Sumi Systems Ltd","Auto Parts"),
    ("533398","MUTHOOTFIN","Muthoot Finance Ltd","Finance"),
    ("532497","NAUKRI","Info Edge India Ltd","Technology"),
    ("526371","NMDC","NMDC Ltd","Mining"),
    ("532555","NTPC","NTPC Ltd","Energy"),
    ("500312","ONGC","Oil and Natural Gas Corp Ltd","Energy"),
    ("532461","PNB","Punjab National Bank","Banking"),
    ("532898","POWERGRID","Power Grid Corp of India Ltd","Energy"),
    ("500113","SAIL","Steel Authority of India Ltd","Metal"),
    ("500387","SHREECEM","Shree Cement Ltd","Cement"),
    ("500550","SIEMENS","Siemens Ltd","Engineering"),
    ("532755","TECHM","Tech Mahindra Ltd","IT"),
    ("500114","TITAN","Titan Company Ltd","Consumer"),
    ("500420","TORNTPHARM","Torrent Pharmaceuticals Ltd","Pharma"),
    ("532538","ULTRACEMCO","UltraTech Cement Ltd","Cement"),
    ("512070","UPL","UPL Ltd","Agro Chemical"),
    ("500295","NESTLEIND","Nestle India Ltd","FMCG"),
    ("543320","ZOMATO","Zomato Ltd","Technology"),
    ("543066","IRCTC","Indian Railway Catering Corp","Travel"),
    ("500251","MRF","MRF Ltd","Auto Parts"),
    ("532286","BERGEPAINT","Berger Paints India Ltd","Consumer"),
    ("500085","CHOLAFIN","Cholamandalam Investment Finance","Finance"),
    ("500483","VOLTAS","Voltas Ltd","Consumer Electricals"),
    ("500146","PAGEIND","Page Industries Ltd","Consumer"),
    ("500331","PIDILITIND","Pidilite Industries Ltd","Chemical"),
]

def seed():
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        admin_email = "admin@ftitrading.com"
        admin_password = "Admin2026"
        existing = db.query(User).filter(User.email == admin_email).first()
        if not existing:
            db.add(User(name="Super Admin", email=admin_email,
                        hashed_password=hash_password(admin_password),
                        is_admin=True, is_active=True, onboarding_done=True))
            db.commit()
            print(f"Admin created: {admin_email} / {admin_password}")
        else:
            print(f"Admin already exists: {admin_email}")

        seen = set()
        added = 0
        for code, sym, name, sector in BSE_STOCKS:
            if code in seen:
                continue
            seen.add(code)
            if not db.query(BSEStock).filter(BSEStock.bse_code == code).first():
                db.add(BSEStock(bse_code=code, symbol=sym, company_name=name, sector=sector))
                added += 1
        db.commit()
        print(f"BSE stocks added: {added}")
        print(f"Total in DB: {db.query(BSEStock).count()}")
        print("\nDatabase seeded successfully!")
        print(f"Email:    {admin_email}")
        print(f"Password: {admin_password}")
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    seed()
