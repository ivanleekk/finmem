import os

# The 32 tickers from your data/03_model_input/ directory
TICKERS = [
    "0700_HK", "1288_HK", "1398_HK", "9988_HK", "AAPL", "ABBV", "AMZN", 
    "AVGO", "AZN_L", "BAC", "BRK-B", "COST", "D05_SI", "HD", "HSBA_L", 
    "JNJ", "JPM", "KO", "LLY", "MCD", "MSFT", "NVDA", "O39_SI", "PG", 
    "RR_L", "SHEL_L", "TSLA", "U11_SI", "UNH", "V", "WMT", "Z74_SI"
]

# Comprehensive map of specific sectors to specific tickers
SECTOR_MAP = {
    # Hong Kong Equities
    "0700_HK": "(1) Video Gaming and Digital Entertainment.\n(2) Social Media and Messaging Networks.\n(3) Cloud Computing and Enterprise Software.\n(4) Fintech and Digital Payments.", # Tencent
    "1288_HK": "(1) Commercial Banking and Lending.\n(2) Agricultural and Rural Finance.\n(3) Retail Banking and Wealth Management.\n(4) Corporate and Institutional Banking.", # AgBank
    "1398_HK": "(1) Global Commercial Banking.\n(2) Corporate Loans and Syndication.\n(3) Asset and Wealth Management.\n(4) Macroeconomic Trends in China.", # ICBC
    "9988_HK": "(1) E-commerce and Retail Technology.\n(2) Cloud Computing Services.\n(3) Digital Media and Entertainment.\n(4) Global Logistics and Supply Chain.", # Alibaba

    # US Tech & E-commerce
    "AAPL": "(1) Consumer Electronics.\n(2) Software and Services Ecosystems.\n(3) Wearable Technology.\n(4) Semiconductor Design.",
    "AMZN": "(1) Global E-commerce.\n(2) Cloud Computing Infrastructure (AWS).\n(3) Digital Streaming and Media.\n(4) Artificial Intelligence and Machine Learning.",
    "MSFT": "(1) Cloud Computing (Azure).\n(2) Enterprise Software and Productivity Tools.\n(3) Artificial Intelligence Integration.\n(4) Personal Computing and Gaming.",
    "NVDA": "(1) Graphics Processing Units (GPUs).\n(2) Artificial Intelligence and Deep Learning Hardware.\n(3) Data Center Infrastructure.\n(4) Gaming and Edge Computing.",
    "AVGO": "(1) Semiconductor Manufacturing.\n(2) Infrastructure and Enterprise Software.\n(3) Networking and Wireless Chips.\n(4) Data Center Storage Solutions.",
    
    # US Healthcare & Pharma
    "ABBV": "(1) Biopharmaceuticals.\n(2) Immunology and Rheumatology.\n(3) Oncology and Therapeutics.\n(4) Neuroscience.",
    "JNJ": "(1) Medical Devices and Technology.\n(2) Pharmaceuticals and Immunology.\n(3) Healthcare Innovation.\n(4) Global Supply Chain in Healthcare.",
    "LLY": "(1) Pharmaceuticals and Drug Discovery.\n(2) Diabetes and Obesity Care (Endocrinology).\n(3) Oncology.\n(4) Neuroscience and Alzheimer's Research.",
    "UNH": "(1) Managed Healthcare and Health Insurance.\n(2) Pharmacy Benefit Management.\n(3) Health Information Technology.\n(4) Clinical Care Delivery Services.",

    # US Financials & Payments
    "BAC": "(1) Retail and Commercial Banking.\n(2) Wealth and Investment Management.\n(3) Global Corporate Banking.\n(4) Investment Banking and Trading.",
    "JPM": "(1) Investment Banking and Capital Markets.\n(2) Commercial Banking.\n(3) Asset & Wealth Management.\n(4) Global Macroeconomics and Fixed Income.",
    "V": "(1) Global Payments Infrastructure.\n(2) Financial Technology (Fintech).\n(3) Digital Transaction Processing.\n(4) Cybersecurity in Finance.",
    "BRK-B": "(1) Insurance and Reinsurance.\n(2) Freight Rail Transportation.\n(3) Utility and Energy Infrastructure.\n(4) Diversified Equity Investments.",

    # US Consumer & Retail
    "COST": "(1) Membership-only Warehouse Retail.\n(2) Supply Chain and Inventory Management.\n(3) Consumer Goods and Groceries.\n(4) Private Label Strategy.",
    "HD": "(1) Home Improvement Retail.\n(2) Construction Materials and Equipment.\n(3) Supply Chain and Logistics.\n(4) U.S. Housing Market Trends.",
    "MCD": "(1) Fast Food and Quick Service Restaurants.\n(2) Commercial Real Estate.\n(3) Global Franchising Models.\n(4) Supply Chain Procurement.",
    "WMT": "(1) Big-box Retail and Grocery.\n(2) E-commerce and Omnichannel Retail.\n(3) Global Supply Chain and Logistics.\n(4) Consumer Discretionary Trends.",
    "KO": "(1) Non-alcoholic Beverages.\n(2) Global Bottling and Distribution Networks.\n(3) Consumer Staples and Brand Marketing.\n(4) Supply Chain Management.",
    "PG": "(1) Consumer Packaged Goods (CPG).\n(2) Personal Care and Hygiene.\n(3) Health and Grooming.\n(4) Brand Management and Retail Distribution.",
    "TSLA": "(1) Electric Vehicles (Automotive Sector).\n(2) Energy Generation and Storage.\n(3) Autonomous Driving and AI Technology.\n(4) Battery Production and Development.",

    # UK/Europe Equities
    "AZN_L": "(1) Biopharmaceuticals.\n(2) Oncology Research and Development.\n(3) Cardiovascular and Metabolic Diseases.\n(4) Respiratory and Immunology.", # AstraZeneca
    "HSBA_L": "(1) Global Banking and Markets.\n(2) Wealth and Personal Banking.\n(3) Commercial Banking and Trade Finance.\n(4) Emerging Market Economics.", # HSBC
    "RR_L": "(1) Civil Aerospace and Aviation.\n(2) Defense Technology.\n(3) Power Systems and Generation.\n(4) Advanced Engineering.", # Rolls-Royce
    "SHEL_L": "(1) Oil and Gas Exploration.\n(2) Renewable Energy and Transition.\n(3) Petrochemicals and Refining.\n(4) Global Energy Markets.", # Shell

    # Singapore Equities
    "D05_SI": "(1) Asian Commercial Banking.\n(2) Wealth Management and Private Banking.\n(3) Digital Banking Innovation.\n(4) Southeast Asian Macroeconomics.", # DBS
    "O39_SI": "(1) Commercial and Retail Banking.\n(2) Life and General Insurance.\n(3) Wealth Management.\n(4) SME Financing in Asia.", # OCBC
    "U11_SI": "(1) Commercial Banking and Lending.\n(2) SME and Corporate Banking.\n(3) Wealth Management.\n(4) Treasury and Global Markets.", # UOB
    "Z74_SI": "(1) Telecommunications Infrastructure.\n(2) Digital Services and Cybersecurity.\n(3) 5G Network Deployment.\n(4) Information and Communications Technology (ICT).", # Singtel

    # Fallback
    "DEFAULT": "(1) Global Macroeconomics.\n(2) Equity Markets.\n(3) Financial Statement Analysis.\n(4) Industry Trend Evaluation."
}

# The TOML template
TEMPLATE = """[chat]
model = "z-ai/glm-4-32b"
end_point = "https://openrouter.ai/api/v1/chat/completions"
system_message = "You are a helpful assistant."

[general]
top_k = 3
agent_name = "agent_1"
look_back_window_size = 7
trading_symbol = "{ticker}"
character_string = '''
You accumulate a lot of information of the following sectors so you are especially good at trading them:
{sectors}

You are an expert of {ticker}.
'''

[agent.agent_1.embedding.detail]
embedding_model = "text-embedding-ada-002"
chunk_size = 5000
verbose = false

[short]
importance_score_initialization = "sample"
decay_params = {{recency_factor=3.0, importance_factor=0.92}}
clean_up_threshold_dict = {{recency_threshold=0.05, importance_threshold=5}}
jump_threshold_upper = 60

[mid]
jump_threshold_lower = 60
jump_threshold_upper = 80
importance_score_initialization = "sample"
decay_params = {{recency_factor=90.0, importance_factor=0.967}}
clean_up_threshold_dict = {{recency_threshold=0.05, importance_threshold=5}}

[long]
jump_threshold_lower = 80
importance_score_initialization = "sample"
decay_params = {{recency_factor=365.0, importance_factor=0.988}}
clean_up_threshold_dict = {{recency_threshold=0.05, importance_threshold=5}}

[reflection]
importance_score_initialization = "sample"
decay_params = {{recency_factor=365.0, importance_factor=0.988}}
clean_up_threshold_dict = {{recency_threshold=0.05, importance_threshold=5}}
"""

def generate_configs():
    # Ensure the output directory exists
    output_dir = "config/generated_configs"
    os.makedirs(output_dir, exist_ok=True)

    for ticker in TICKERS:
        # Fetch specific sectors or fall back to default
        sectors = SECTOR_MAP.get(ticker, SECTOR_MAP["DEFAULT"])
        
        # Format the template
        toml_content = TEMPLATE.format(ticker=ticker, sectors=sectors)
        
        file_path = os.path.join(output_dir, f"{ticker}_config.toml")
        
        with open(file_path, "w") as f:
            f.write(toml_content)
            
    print(f"Successfully generated {len(TICKERS)} TOML files in {output_dir}/")

if __name__ == "__main__":
    generate_configs()
