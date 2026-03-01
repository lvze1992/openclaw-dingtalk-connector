/**
 * 从钉钉下载机器人接收的媒体文件（图片/文件）
 * 使用 api.dingtalk.com 新版接口 /v1.0/robot/messageFiles/download
 */

import axios from 'axios';
import { getAccessToken } from './tokens';

const SUPPORTED_IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'];

export function isImageFileByName(filename: string): boolean {
  const ext = filename.toLowerCase().slice(filename.lastIndexOf('.'));
  return SUPPORTED_IMAGE_EXTENSIONS.includes(ext);
}

/**
 * 使用 downloadCode 从钉钉下载媒体文件
 * @param config 钉钉配置（clientId 等，用于 getAccessToken 与 robotCode）
 * @param downloadCode 钉钉提供的下载码（图片 pictureDownloadCode，文件 downloadCode）
 * @param fileName 文件名（用于扩展名）
 * @param log 可选日志
 */
export async function downloadMediaFromDingTalk(
  config: { clientId: string; clientSecret: string },
  downloadCode: string,
  fileName: string,
  log?: { info?: (m: string) => void; error?: (m: string) => void },
): Promise<string | null> {
  try {
    const fs = await import('fs');
    const path = await import('path');
    const os = await import('os');

    const apiToken = await getAccessToken(config);
    log?.info?.(`[DingTalk][Download] 获取下载URL: downloadCode=${downloadCode.slice(0, 20)}...`);

    const downloadUrlResp = await axios.post(
      'https://api.dingtalk.com/v1.0/robot/messageFiles/download',
      { downloadCode, robotCode: config.clientId },
      {
        headers: { 'Content-Type': 'application/json', 'x-acs-dingtalk-access-token': apiToken },
        timeout: 30_000,
      },
    );

    const d = downloadUrlResp.data;
    const downloadUrl =
      d?.downloadUrl ??
      d?.download_url ??
      d?.result?.downloadUrl ??
      d?.body?.downloadUrl ??
      (typeof d === 'string' ? d : null);
    if (!downloadUrl) {
      log?.error?.(`[DingTalk][Download] 下载URL为空，响应: ${JSON.stringify(d).slice(0, 200)}`);
      return null;
    }

    log?.info?.(`[DingTalk][Download] 获取到下载URL: ${String(downloadUrl).slice(0, 100)}...`);

    const fileResp = await axios.get(downloadUrl, {
      responseType: 'arraybuffer',
      timeout: 60_000,
      maxContentLength: 50 * 1024 * 1024,
    });

    let ext = path.extname(fileName);
    if (!ext) {
      const contentType = fileResp.headers['content-type'];
      if (contentType?.includes('image/jpeg') || contentType?.includes('image/jpg')) ext = '.jpg';
      else if (contentType?.includes('image/png')) ext = '.png';
      else if (contentType?.includes('image/gif')) ext = '.gif';
      else if (contentType?.includes('application/pdf')) ext = '.pdf';
      else ext = '';
    }

    const inboundDir = path.join(os.homedir(), '.openclaw', 'media', 'inbound');
    if (!fs.existsSync(inboundDir)) {
      fs.mkdirSync(inboundDir, { recursive: true });
    }

    const timestamp = Date.now();
    const safeFileName = fileName.replace(/[^a-zA-Z0-9._-]/g, '_');
    const baseName =
      ext && safeFileName.toLowerCase().endsWith(ext.toLowerCase())
        ? safeFileName.slice(0, -ext.length)
        : safeFileName;
    const localFileName = `dingtalk_${timestamp}_${baseName}${ext}`;
    const localPath = path.join(inboundDir, localFileName);

    const buf = Buffer.from(fileResp.data as ArrayBuffer);
    fs.writeFileSync(localPath, buf);

    const fileSizeMB = (buf.length / (1024 * 1024)).toFixed(2);
    log?.info?.(`[DingTalk][Download] 文件下载成功: ${localPath} (${fileSizeMB}MB)`);

    return localPath;
  } catch (err: any) {
    log?.error?.(`[DingTalk][Download] 下载失败: ${err.message}`);
    if (err.response) {
      log?.error?.(
        `[DingTalk][Download] 错误响应: status=${err.response.status} data=${JSON.stringify(err.response.data)}`,
      );
    }
    return null;
  }
}
