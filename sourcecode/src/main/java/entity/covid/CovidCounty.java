package entity.covid;

import java.io.Serializable;

import scala.Tuple2;

public class CovidCounty implements Serializable {
	private static final long serialVersionUID = 1L;
	private String date;
	private String county;
	private String state;
	private Integer fips;
	private Integer cases;
	private Integer deaths;
	public CovidCounty() {
		super();
	}
	public CovidCounty(String date, String county, String state, Integer fips, Integer cases, Integer deaths) {
		super();
		this.date = date;
		this.county = county;
		this.state = state;
		this.fips = fips;
		this.cases = cases;
		this.deaths = deaths;
	}
	
	public static CovidCounty parse(Tuple2<String, String> tp2) {
		return parse(tp2._2());
	}
	
	public static CovidCounty parse(String row) {
		String[] strs = row.split(",");
		if (strs.length == 6) {
			return new CovidCounty(strs[0], strs[1], strs[2], Integer.valueOf(strs[3]), Integer.valueOf(strs[4]), Integer.valueOf(strs[5]));
		}
		return new CovidCounty();
	}
	
	public String getKey() {
		StringBuilder key = new StringBuilder();
		key.append(date).append("|").append(fips);
		return key.toString();
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getCounty() {
		return county;
	}
	public void setCounty(String county) {
		this.county = county;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public Integer getFips() {
		return fips;
	}
	public void setFips(Integer fips) {
		this.fips = fips;
	}
	public Integer getCases() {
		return cases;
	}
	public void setCases(Integer cases) {
		this.cases = cases;
	}
	public Integer getDeaths() {
		return deaths;
	}
	public void setDeaths(Integer deaths) {
		this.deaths = deaths;
	}
	
}
