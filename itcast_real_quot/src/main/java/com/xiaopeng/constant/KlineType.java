package com.xiaopeng.constant;

public enum KlineType {

	DAYK("1", "trade_date"), WEEKK("2", "week_first_txdate"), MONTHK("3", "month_first_txdate"), YEARK("4",
			"year_first_txdate");

	private String type;//K线类型
	private String firstTxDateType; //周期的首个交易日

	private KlineType(String type, String firstTxDateType) {
		this.type = type;
		this.firstTxDateType = firstTxDateType;
	}

	public String getType() {
		return type;
	}

	public String getFirstTxDateType() {
		return firstTxDateType;
	}

}
