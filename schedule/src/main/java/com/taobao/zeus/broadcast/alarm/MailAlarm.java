package com.taobao.zeus.broadcast.alarm;

import java.util.List;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;

import com.taobao.zeus.store.mysql.persistence.ZeusUser;

import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;

public class MailAlarm extends AbstractZeusAlarm {
	@Autowired
	@Qualifier("mailSender")
	private JavaMailSenderImpl mailSender;

	

	@Override
	public void alarm(List<String> users, String title, String content) throws Exception {
		// TODO to be implements
		List<ZeusUser> zeusUsers = userManager.findListByUid(users);
		for (ZeusUser user : zeusUsers) {
			ScheduleInfoLog.info("Send Message:" + mailSender.getUsername() + " to " + user.getEmail());
			sendHtml(mailSender.getUsername(), user.getEmail(), "Zeus Warning- " + title, content);
		}

	}

	private void sendHtml(String mailFrom, String mailTo, String tile,
			String content) throws MessagingException {
		MimeMessage mailMessage = mailSender.createMimeMessage();
		MimeMessageHelper messageHelper = new MimeMessageHelper(mailMessage,
				true, "utf-8");
		messageHelper.setFrom(mailFrom);
		messageHelper.setTo(mailTo);
		messageHelper.setSubject(tile);
		messageHelper.setText("<html><head></head><body>" + content
				+ "</body></html>", true);
		mailSender.send(mailMessage);
	}
}
