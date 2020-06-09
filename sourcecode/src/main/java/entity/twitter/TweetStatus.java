package entity.twitter;
import java.io.Serializable;

public class TweetStatus implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String text, userName, screenName, createAt;

	public TweetStatus(String text, String userName, String screenName, String createAt) {
		super();
		this.text = text;
		this.userName = userName;
		this.screenName = screenName;
		this.createAt = createAt;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}

	public String getCreateAt() {
		return createAt;
	}

	public void setCreateAt(String createAt) {
		this.createAt = createAt;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getText())
			.append(",").append(getUserName())
			.append(",").append(getScreenName())
			.append(",").append(getCreateAt());
		return sb.toString();
	}
	
	
}
