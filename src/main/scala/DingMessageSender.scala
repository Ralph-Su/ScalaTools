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
  def sendMarkdownMessage(title : String, content : String) : Unit = {
     val webhook = "https://oapi.dingtalk.com/robot/send?access_token=***"
     val client : DingTalkClient= new DefaultDingTalkClient(webhook);
     val request: OapiRobotSendRequest = new OapiRobotSendRequest();
//    request.setMsgtype("text");
//    OapiRobotSendRequest.Text text = new OapiRobotSendRequest.Text();
//    text.setContent("测试文本消息");
//    request.setText(text);
//    OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();
//    at.setAtMobiles(Arrays.asList("132xxxxxxxx"));
//    // isAtAll类型如果不为Boolean，请升级至最新SDK
//    at.setIsAtAll(true);
//    at.setAtUserIds(Arrays.asList("109929","32099"));
//    request.setAt(at);

//    request.setMsgtype("link");
//    OapiRobotSendRequest.Link link = new OapiRobotSendRequest.Link();
//    link.setMessageUrl("https://www.dingtalk.com/");
//    link.setPicUrl("");
//    link.setTitle("时代的火车向前开");
//    link.setText("这个即将发布的新版本，创始人xx称它为红树林。而在此之前，每当面临重大升级，产品经理们都会取一个应景的代号，这一次，为什么是红树林");
//    request.setLink(link);

    request.setMsgtype("markdown")
    val markdown : OapiRobotSendRequest.Markdown = new OapiRobotSendRequest.Markdown()
    markdown.setTitle(title)
    markdown.setText(content)
    request.setMarkdown(markdown)

//    val at : OapiRobotSendRequest.At= new OapiRobotSendRequest.At();
//    at.setAtMobiles(Arrays.asList("17333144775"));
      // isAtAll类型如果不为Boolean，请升级至最新SDK
//    at.setIsAtAll(true);
//    at.setAtUserIds(Arrays.asList("109929","32099"))
//    request.setAt(at);

    val response : OapiRobotSendResponse  = client.execute(request)
    if (response.isSuccess) println("钉钉消息推送完成。")
    println(response.getErrmsg, response.getErrcode, response.isSuccess)

  }

  val title = "Data Task Completed"
  val content = "中国区产线数据生产完成!"
  sendMarkdownMessage(title, content)

}
