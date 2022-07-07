from datetime import datetime
from pydantic import BaseModel


class Holding(BaseModel):
    portfolio_code            :str      = None
    date                      :str      = None
    security_code             :str      = None
    strategy_type_name        :str      = None
    reference                 :str      = None
    quantity                  :float    = None
    clean_price               :float    = None
    dirty_price               :float    = None
    exchange_rate             :float    = None
    dirty_market_value_base   :float    = None
    dirty_market_value_local  :float    = None
    accrued_interest_base     :float    = None
    accrued_interest_local    :float    = None
    exposure_base             :float    = None
    exposure_local            :float    = None
    dirty_book_value_base     :float    = None
    dirty_book_value_local    :float    = None
    price_date                :datetime = None
