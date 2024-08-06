from typing import Dict, List


def test_function() -> List[Dict]:
    mock_trades = [
        {
            'product_id': 'BTC-USD',
            'price': 60000,
            'volume': 0.01,
            'timestamp': 1630000000,
        },
        {
            'product_id': 'BTC-USD',
            'price': 59000,
            'volume': 0.01,
            'timestamp': 1640000000,
        },
    ]
    return mock_trades


if __name__ == '__main__':
    trades = test_function()
    print(trades)
