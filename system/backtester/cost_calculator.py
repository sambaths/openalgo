"""
Transaction Cost Calculator for Trading

This module provides functionality to calculate transaction costs
including brokerage fees and regulatory charges for trades.
"""

import yaml
from dataclasses import dataclass
from typing import Dict, Any, Tuple
from pathlib import Path


@dataclass
class FeeConfig:
    """Fee configuration for transaction cost calculations"""
    # Brokerage fees
    brokerage_pct: float
    brokerage_cap: float
    
    # Regulatory charges
    exchange_transaction_pct: float
    gst_pct: float
    stt_pct: float
    sebi_pct: float
    stamp_duty_pct: float
    nse_ipft_pct: float
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'FeeConfig':
        """Create a FeeConfig from a dictionary"""
        # Handle different configuration structures
        
        # Check if the config has a nested structure with transaction_costs
        if 'risk_management' in config_dict and 'transaction_costs' in config_dict['risk_management']:
            transaction_costs = config_dict['risk_management']['transaction_costs']
            commissions = transaction_costs.get('commissions', {})
            regulatory_charges = transaction_costs.get('regulatory_charges', {})
        else:
            # Use the direct structure (for backward compatibility)
            commissions = config_dict.get('commissions', {})
            regulatory_charges = config_dict.get('regulatory_charges', {})
        
        return cls(
            # Brokerage fees
            brokerage_pct=commissions.get('brokerage_pct', 0) / 100,
            brokerage_cap=commissions.get('brokerage_cap', float('inf')),
            
            # Regulatory charges - convert percentages to decimals
            exchange_transaction_pct=regulatory_charges.get('exchange_transaction_pct', 0) / 100,
            gst_pct=regulatory_charges.get('GST_pct', 0) / 100,
            stt_pct=regulatory_charges.get('STT_pct', 0) / 100,
            sebi_pct=regulatory_charges.get('SEBI_pct', 0) / 100,
            stamp_duty_pct=regulatory_charges.get('stamp_duty_pct', 0) / 100,
            nse_ipft_pct=regulatory_charges.get('NSE_IPFT_pct', 0) / 100
        )
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'FeeConfig':
        """Load fee configuration from a YAML file"""
        try:
            with open(yaml_path, 'r') as f:
                config_dict = yaml.safe_load(f)
            return cls.from_dict(config_dict)
        except Exception as e:
            raise ValueError(f"Error loading fee config from {yaml_path}: {str(e)}")


class TransactionCostCalculator:
    """Calculator for trading transaction costs and P&L"""
    
    def __init__(self, fee_config: FeeConfig):
        """
        Initialize with fee configuration
        
        Args:
            fee_config: Fee configuration object
        """
        self.fee_config = fee_config
    
    def calculate_costs(self, buy_price: float, sell_price: float, quantity: int) -> Dict[str, Any]:
        """
        Calculate all transaction costs and P&L for a trade
        
        Args:
            buy_price: Price at which the asset was purchased
            sell_price: Price at which the asset was sold
            quantity: Number of units traded
            
        Returns:
            Dictionary with detailed breakdown of costs and P&L
        """
        # Calculate turnovers
        buy_turnover = buy_price * quantity
        sell_turnover = sell_price * quantity
        total_turnover = buy_turnover + sell_turnover
        
        # Calculate brokerage fees
        buy_brokerage = min(buy_turnover * self.fee_config.brokerage_pct, self.fee_config.brokerage_cap)
        sell_brokerage = min(sell_turnover * self.fee_config.brokerage_pct, self.fee_config.brokerage_cap)
        total_brokerage = buy_brokerage + sell_brokerage
        
        # Calculate regulatory charges
        exchange_transaction_fee = total_turnover * self.fee_config.exchange_transaction_pct
        gst_fee = total_turnover * self.fee_config.gst_pct
        stt_fee = total_turnover * self.fee_config.stt_pct
        sebi_fee = total_turnover * self.fee_config.sebi_pct
        stamp_duty_fee = total_turnover * self.fee_config.stamp_duty_pct
        nse_ipft_fee = total_turnover * self.fee_config.nse_ipft_pct
        
        # Sum up regulatory charges
        total_regulatory_charges = (
            exchange_transaction_fee +
            gst_fee +
            stt_fee +
            sebi_fee +
            stamp_duty_fee +
            nse_ipft_fee
        )
        
        # Calculate total charges
        total_charges = total_brokerage + total_regulatory_charges
        
        # Calculate P&L
        gross_pnl = (sell_price - buy_price) * quantity
        net_pnl = gross_pnl - total_charges
        
        # Calculate breakeven point
        breakeven_per_share = total_charges / quantity if quantity > 0 else 0
        
        # Return detailed results
        return {
            "turnovers": {
                "buy_turnover": buy_turnover,
                "sell_turnover": sell_turnover,
                "total_turnover": total_turnover
            },
            "brokerage": {
                "buy_brokerage": buy_brokerage,
                "sell_brokerage": sell_brokerage,
                "total_brokerage": total_brokerage
            },
            "regulatory_charges": {
                "exchange_transaction_fee": exchange_transaction_fee,
                "gst_fee": gst_fee,
                "stt_fee": stt_fee,
                "sebi_fee": sebi_fee,
                "stamp_duty_fee": stamp_duty_fee,
                "nse_ipft_fee": nse_ipft_fee,
                "total_regulatory_charges": total_regulatory_charges
            },
            "totals": {
                "total_charges": total_charges,
                "gross_pnl": gross_pnl,
                "net_pnl": net_pnl,
                "breakeven_per_share": breakeven_per_share
            }
        }


def calculate_transaction_costs(
    buy_price: float, 
    sell_price: float, 
    quantity: int,
    config_path: str = None,
    config_dict: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Convenience function to calculate transaction costs
    
    Args:
        buy_price: Price at which the asset was purchased
        sell_price: Price at which the asset was sold
        quantity: Number of units traded
        config_path: Path to YAML configuration file (optional)
        config_dict: Dictionary with fee configuration (optional)
        
    Returns:
        Dictionary with detailed breakdown of costs and P&L
        
    Note:
        Either config_path or config_dict must be provided.
    """
    # Load fee configuration
    if config_path:
        fee_config = FeeConfig.from_yaml(config_path)
    elif config_dict:
        fee_config = FeeConfig.from_dict(config_dict)
    else:
        raise ValueError("Either config_path or config_dict must be provided")
    
    # Calculate costs
    calculator = TransactionCostCalculator(fee_config)
    return calculator.calculate_costs(buy_price, sell_price, quantity)


def format_cost_breakdown(costs: Dict[str, Any]) -> str:
    """
    Format cost breakdown as a readable string
    
    Args:
        costs: Cost breakdown dictionary from calculate_transaction_costs
        
    Returns:
        Formatted string with cost breakdown
    """
    lines = []
    lines.append("=== TRANSACTION COST BREAKDOWN ===")
    
    # Format turnovers
    lines.append("\nTURNOVERS:")
    lines.append(f"  Buy Turnover:   ₹{costs['turnovers']['buy_turnover']:.2f}")
    lines.append(f"  Sell Turnover:  ₹{costs['turnovers']['sell_turnover']:.2f}")
    lines.append(f"  Total Turnover: ₹{costs['turnovers']['total_turnover']:.2f}")
    
    # Format brokerage
    lines.append("\nBROKERAGE FEES:")
    lines.append(f"  Buy Brokerage:   ₹{costs['brokerage']['buy_brokerage']:.2f}")
    lines.append(f"  Sell Brokerage:  ₹{costs['brokerage']['sell_brokerage']:.2f}")
    lines.append(f"  Total Brokerage: ₹{costs['brokerage']['total_brokerage']:.2f}")
    
    # Format regulatory charges
    lines.append("\nREGULATORY CHARGES:")
    lines.append(f"  Exchange Transaction Fee: ₹{costs['regulatory_charges']['exchange_transaction_fee']:.2f}")
    lines.append(f"  GST Fee:                  ₹{costs['regulatory_charges']['gst_fee']:.2f}")
    lines.append(f"  STT Fee:                  ₹{costs['regulatory_charges']['stt_fee']:.2f}")
    lines.append(f"  SEBI Fee:                 ₹{costs['regulatory_charges']['sebi_fee']:.2f}")
    lines.append(f"  Stamp Duty Fee:           ₹{costs['regulatory_charges']['stamp_duty_fee']:.2f}")
    lines.append(f"  NSE IPFT Fee:             ₹{costs['regulatory_charges']['nse_ipft_fee']:.2f}")
    lines.append(f"  Total Regulatory Charges: ₹{costs['regulatory_charges']['total_regulatory_charges']:.2f}")
    
    # Format totals
    lines.append("\nRESULTS:")
    lines.append(f"  Total Charges:       ₹{costs['totals']['total_charges']:.2f}")
    lines.append(f"  Gross P&L:           ₹{costs['totals']['gross_pnl']:.2f}")
    lines.append(f"  Net P&L:             ₹{costs['totals']['net_pnl']:.2f}")
    lines.append(f"  Breakeven per share: ₹{costs['totals']['breakeven_per_share']:.2f}")
    
    return "\n".join(lines)


# Add a test function for backtester config
def test_backtester_config():
    """Test transaction cost calculator with backtester config."""
    from pathlib import Path
    
    # Find the backtester config file
    config_path = Path(__file__).parent / "backtester_config.yaml"
    
    if not config_path.exists():
        print(f"Backtester config file not found at {config_path}")
        return
    
    # Example trade
    buy_price = 100.0
    sell_price = 105.0
    quantity = 100
    
    # Calculate costs using the backtester config
    costs = calculate_transaction_costs(
        buy_price=buy_price,
        sell_price=sell_price,
        quantity=quantity,
        config_path=str(config_path)
    )
    
    # Print breakdown
    print(format_cost_breakdown(costs))


# Example usage
if __name__ == "__main__":
    # Example fee configuration
    example_config = {
        "commissions": {
            "brokerage_pct": 0.03,    # 0.03% fee per trade leg
            "brokerage_cap": 20       # Maximum charge of ₹20 per leg
        },
        "regulatory_charges": {
            "exchange_transaction_pct": 0.00297,  # 0.00297% of turnover
            "GST_pct": 0.00069,                   # 0.00069% of turnover
            "STT_pct": 0.01321,                   # 0.01321% of turnover
            "SEBI_pct": 0.0001,                   # 0.0001% of turnover
            "stamp_duty_pct": 0.00142,            # 0.00142% of turnover
            "NSE_IPFT_pct": 0.0001                # 0.0001% of turnover
        }
    }
    
    # Example trade
    buy_price = 100.0
    sell_price = 105.0
    quantity = 100
    
    # Calculate costs
    costs = calculate_transaction_costs(
        buy_price=buy_price,
        sell_price=sell_price,
        quantity=quantity,
        config_dict=example_config
    )
    
    # Print breakdown
    print(format_cost_breakdown(costs))
    
    # Test with backtester config
    print("\nTesting with backtester config:")
    test_backtester_config() 