package com.xiaopeng.util;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.File;
import java.io.FileOutputStream;

/**
 * 下载工具类
 */
public class DownloadUtil {

    /**
     * FTP文件下载
     */
    public static boolean download(String host,int port,String userName,String password,String ftpDirectory, String localDirectory) {

        boolean flag = false;
        //1.初始化ftp连接
        FTPClient ftpClient = initFtpClient(host,port,userName,password);
        try {
            //2.切换工作目录
            ftpClient.changeWorkingDirectory(ftpDirectory);
            //被动模式，服务端开放端口，用于数据传输
            ftpClient.enterLocalPassiveMode();
            //禁用服务端参与的验证，如果不禁用服务端会获取主机IP与提交的host进行匹配，不一致时会报错
            ftpClient.setRemoteVerificationEnabled(false);
            //3.获取工作目录的文件信息列表
            FTPFile[] ftpFiles = ftpClient.listFiles();
            for (FTPFile ftpFile : ftpFiles) {
                File file = new File(localDirectory + "/" + ftpFile.getName());
                FileOutputStream fos = new FileOutputStream(file);
                //4.文件输出
                ftpClient.retrieveFile(ftpFile.getName(), fos);
                fos.close();
            }
            ftpClient.logout();
            System.out.println("文件下载成功。。。。");
            //5.返回成功状态
            flag = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return flag;
    }

    /**
     * 初始化ftp连接
     */
    public static FTPClient initFtpClient(String host,int port,String userName,String password) {
        FTPClient ftpClient = new FTPClient();
        ftpClient.setControlEncoding("UTF-8");
        try {
            ftpClient.connect(host, port);
            ftpClient.login(userName, password);
            //FTP服务器检查连接是否成功,220表示连接成功，230登陆成功，221再见
            int replyCode = ftpClient.getReplyCode();
            //判断连接是否成功响应
            if (!FTPReply.isPositiveCompletion(replyCode)) {
                System.out.println("connect ftp sever failed ");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ftpClient;
    }
}
