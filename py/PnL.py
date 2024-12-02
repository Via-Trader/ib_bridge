from ib_insync import IB, Position
from datetime import datetime

def calculate_daily_pnl(ib):
    """
    Calculate the realized profit and loss (P&L) for the current trading day.

    Args:
        ib (IB): An instance of the IB class for interacting with Interactive Brokers API.

    Returns:
        float: The total realized P&L for the day.
    """
    today_str = datetime.now().strftime('%Y%m%d')
    executions = ib.reqExecutions()  # Returns a list of executions
    daily_pnl = 0.0

    for execution in executions:  # Iterate through the list of executions
        if execution.time.strftime('%Y%m%d') == today_str:  # Match today's date
            multiplier = int(execution.contract.multiplier) if execution.contract.multiplier else 1
            trade_pnl = (execution.price * execution.shares * multiplier)
            if execution.side == 'SELL':
                trade_pnl = -trade_pnl
            daily_pnl += trade_pnl

    return daily_pnl

def calculate_unrealized_pnl(ib):
    """
    Calculate the unrealized P&L for open positions.

    Args:
        ib (IB): An instance of the IB class for interacting with Interactive Brokers API.

    Returns:
        float: The total unrealized P&L for open positions.
    """
    unrealized_pnl = 0.0
    positions = ib.positions()

    for position in positions:
        contract = position.contract
        market_price = ib.reqMktData(contract).last  # Fetch the latest market price
        ib.sleep(2)  # Allow data to populate
        if market_price > 0:
            # Calculate P&L: (Market Price - Average Cost) * Quantity * Multiplier
            multiplier = int(contract.multiplier) if contract.multiplier else 1
            position_pnl = (market_price - position.avgCost) * position.position * multiplier
            unrealized_pnl += position_pnl
        else:
            print(f"Market price not available for {contract.symbol}.")

    return unrealized_pnl

def calculate_total_pnl(ib):
    """
    Calculate both realized and unrealized P&L.

    Args:
        ib (IB): An instance of the IB class for interacting with Interactive Brokers API.

    Returns:
        tuple: Realized P&L and Unrealized P&L for the day.
    """
    realized_pnl = calculate_daily_pnl(ib)
    unrealized_pnl = calculate_unrealized_pnl(ib)
    return realized_pnl, unrealized_pnl

if __name__ == "__main__":
    ib = IB()
    try:
        # Connect to IB Gateway or TWS
        ib.connect('127.0.0.1', 7496, clientId=1)
        
        # Calculate P&L
        realized_pnl, unrealized_pnl = calculate_total_pnl(ib)

        # Print results
        print(f"Realized P&L for the day: ${realized_pnl:.2f}")
        print(f"Unrealized P&L: ${unrealized_pnl:.2f}")
        print(f"Total P&L: ${realized_pnl + unrealized_pnl:.2f}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Disconnect from IB
        ib.disconnect()
