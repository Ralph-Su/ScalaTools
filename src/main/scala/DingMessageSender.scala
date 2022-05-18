import com.dingtalk.api.request.OapiRobotSendRequest
import com.dingtalk.api.response.OapiRobotSendResponse
import com.dingtalk.api.{DefaultDingTalkClient, DingTalkClient}
import java.util.Arrays


/**
  * @Author: suwenjin
  * @Description: Scala推送钉钉消息, 参考：https://open.dingtalk.com/document/group/custom-robot-access
  * @Time: 2022/5/8 6:21 PM
**/
object DingMessageSender extends App {

  /**
    * @Description: 发送markdown格式的钉钉消息
    * @Param title: title
    * @Param content: 钉钉消息内容
    * @return: void
  **/
  def sendMarkdownMessage(title : String, content : String) : Boolean = {
     val webhook = "https://oapi.dingtalk.com/robot/send?access_token=***"
     val client : DingTalkClient= new DefaultDingTalkClient(webhook)
     val request: OapiRobotSendRequest = new OapiRobotSendRequest()

    // 构造消息
    request.setMsgtype("markdown")
    val markdown : OapiRobotSendRequest.Markdown = new OapiRobotSendRequest.Markdown()
    markdown.setTitle(title)
    markdown.setText(content)
    request.setMarkdown(markdown)

    // 设置@某个人或者全体成员
    val at : OapiRobotSendRequest.At= new OapiRobotSendRequest.At();
    at.setAtMobiles(Arrays.asList("173xxxx4775"))
    // isAtAll类型如果不为Boolean，请升级至最新SDK
    at.setIsAtAll(false);
    // at.setAtUserIds(Arrays.asList("109929","32099"))
    request.setAt(at);

    // 发送消息
    val response : OapiRobotSendResponse  = client.execute(request)
    println(response.getErrcode, response.getErrmsg)
    response.isSuccess
  }

  /**
    * @Description: 发送Text类型的钉钉消息
    * @Param content: 消息内容
    * @return: void
  **/
  def sendTextMessage(content : String): Boolean = {
    val webhook = "https://oapi.dingtalk.com/robot/send?access_token="
    val client : DingTalkClient= new DefaultDingTalkClient(webhook)
    val request: OapiRobotSendRequest = new OapiRobotSendRequest()

    // 构造消息
    request.setMsgtype("text")
    val text : OapiRobotSendRequest.Text = new OapiRobotSendRequest.Text();
    text.setContent(content)
    request.setText(text)

    // 发送消息
    val response = client.execute(request)
    println(response.getErrcode, response.getErrmsg)
    response.isSuccess
  }

//  sendTextMessage("数据")

  def sendLinkMessage() : Unit = {
    val webhook = "https://oapi.dingtalk.com/robot/send?access_token="
    val client : DingTalkClient= new DefaultDingTalkClient(webhook)
    val request: OapiRobotSendRequest = new OapiRobotSendRequest()

    request.setMsgtype("link")
    val link : OapiRobotSendRequest.Link = new OapiRobotSendRequest.Link();
    link.setMessageUrl("https://www.dingtalk.com/");
    link.setPicUrl("")
    link.setTitle("数据时代的火车向前开")
    link.setText("这个即将发布的新版本，创始人xx称它为红树林。而在此之前，每当面临重大升级，产品经理们都会取一个应景的代号，这一次，为什么是红树林")
    request.setLink(link)

    // 发送消息
    val response = client.execute(request)
    println(response.getErrcode, response.getErrmsg)
    response.isSuccess
  }

}
