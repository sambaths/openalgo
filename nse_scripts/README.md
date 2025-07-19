# NSE Data Fetcher Scripts

## ⚠️ IMPORTANT LEGAL DISCLAIMER

**These scripts are provided for EDUCATIONAL and RESEARCH purposes ONLY.**

### 🎓 Educational Purpose
- Learning about data retrieval techniques
- Understanding web scraping methodologies  
- Analyzing financial market data structures
- Research and academic purposes

### 🚫 What These Scripts Are NOT For
- ❌ Financial advice or investment recommendations
- ❌ Commercial trading systems or algorithms
- ❌ Production trading environments
- ❌ Automated trading decisions
- ❌ Real-time trading strategies

### ⚖️ Legal and Risk Warnings

1. **User Responsibility**: You assume ALL responsibility and risk for using these scripts
2. **No Warranties**: Authors provide NO guarantees about data accuracy or completeness
3. **Compliance**: Users must ensure compliance with all applicable laws and NSE terms of service
4. **Indemnification**: Users agree to hold authors harmless from any legal consequences
5. **Data Usage**: Respect website terms of service and rate limiting policies

## 📁 Scripts Included

### 1. `fetch_index_constituents.py`
Retrieves constituents of major NSE indices including:
- NIFTY 50, NEXT 50, 100, 200, 500
- MIDCAP and SMALLCAP indices
- Sectoral indices (BANK, IT, PHARMA, etc.)

**Output**: `data/index_constituents.csv` with market cap classifications

### 2. `fetch_active_stocks.py`
Fetches real-time market activity data:
- Most active stocks by value/volume
- Volume gainers
- Top gainers and losers
- Bulk deals, block deals, short deals

**Output**: `data/active_stocks_with_reasons.csv` with categorized data

## 🚀 Usage

### Prerequisites
```bash
pip install requests pandas tqdm
```

### Running Scripts
```bash
# Navigate to nse_scripts folder
cd nse_scripts

# Run either script (will show disclaimer first)
python fetch_index_constituents.py
python fetch_active_stocks.py
```

### User Acknowledgment Required
Both scripts display a comprehensive disclaimer before execution:
- Shows comprehensive disclaimer and terms
- **By executing the script, you automatically acknowledge and agree to all terms**
- Users are advised to stop execution if they don't agree to the terms
- No interactive prompt required - execution implies consent

## 🛡️ Safety Features

### Bot Detection Bypass
- Realistic browser headers
- Multi-step session establishment
- Human-like timing patterns
- Progressive retry backoff
- Rate limiting respect

### Error Handling
- Graceful failure handling
- Session refresh on authentication errors
- Network timeout management
- Comprehensive logging

## 📊 Data Structure

### Index Constituents Output
```csv
Index,Index Type,Stock,symbol,industry,isin,...,Market Cap Classification
NIFTY 50,INDEX,RELIANCE,RELIANCE,Oil & Gas,...,Large Cap
```

### Active Stocks Output  
```csv
symbol,companyName,basePrice,...,Reason,relevant_columns
TCS,Tata Consultancy Services,4155,...,Most Active by Value,symbol,companyName,basePrice
```

## ⚠️ Terms of Use

By using these scripts, you acknowledge:

1. **Educational Use Only**: Scripts are for learning and research purposes
2. **No Financial Advice**: No investment recommendations provided
3. **User Risk**: All usage risks are assumed by the user
4. **Legal Compliance**: Users must comply with all applicable laws
5. **No Commercial Use**: Not intended for commercial applications
6. **Data Accuracy**: No guarantees about data accuracy or timeliness
7. **Indemnification**: Users hold authors harmless from any claims

## 🔒 Privacy and Security

- Scripts only access publicly available NSE data
- No personal information collected or stored
- Local data processing only
- Respects website terms of service

## 📞 Support

For educational questions or issues:
- Review the script documentation
- Check NSE's official API documentation
- Ensure compliance with usage policies

**Remember**: These tools are for learning purposes only. Always consult qualified financial advisors for investment decisions.

---

**Disclaimer**: Authors are not responsible for any financial losses, legal issues, or damages arising from the use of these educational scripts. Use at your own risk and discretion. 