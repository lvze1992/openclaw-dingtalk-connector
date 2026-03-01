/**
 * 回复内容中的特殊格式处理
 *
 * 1. 媒体 System Prompt：告诉模型如何输出图片/视频/音频/文件
 * 2. 本地图片路径：识别并上传到钉钉，替换为 media_id
 * 3. DINGTALK_* 标记：解析 [DINGTALK_FILE]、[DINGTALK_VIDEO]、[DINGTALK_AUDIO]，上传并发送独立消息
 */

import axios from 'axios';

// ---------- 导出的正则，供主流程流式节流时清理展示内容 ----------
export const FILE_MARKER_PATTERN = /\[DINGTALK_FILE\]({.*?})\[\/DINGTALK_FILE\]/g;
export const VIDEO_MARKER_PATTERN = /\[DINGTALK_VIDEO\]({.*?})\[\/DINGTALK_VIDEO\]/g;
export const AUDIO_MARKER_PATTERN = /\[DINGTALK_AUDIO\]({.*?})\[\/DINGTALK_AUDIO\]/g;

/** 投放目标（与主文件 AICardTarget 结构一致，避免循环依赖） */
export type MediaFormatTarget =
  | { type: 'user'; userId: string }
  | { type: 'group'; openConversationId: string };

/** 获取 api.dingtalk.com access_token，主动发消息时由主文件传入 */
export type GetAccessToken = (config: any) => Promise<string>;

// ---------- 1. 媒体 System Prompt ----------
export function buildMediaSystemPrompt(): string {
  return `## 钉钉图片和文件显示规则

你正在钉钉中与用户对话。

### 一、图片显示

显示图片时，直接使用本地文件路径，系统会自动上传处理。

**正确方式**：
\`\`\`markdown
![描述](file:///path/to/image.jpg)
![描述](/tmp/screenshot.png)
![描述](/Users/xxx/photo.jpg)
\`\`\`

**禁止**：
- 不要自己执行 curl 上传
- 不要猜测或构造 URL
- **不要对路径进行转义（如使用反斜杠 \\ ）**

直接输出本地路径即可，系统会自动上传到钉钉。

### 二、视频分享

**何时分享视频**：
- ✅ 用户明确要求**分享、发送、上传**视频时
- ❌ 仅生成视频保存到本地时，**不需要**分享

**视频标记格式**：
当需要分享视频时，在回复**末尾**添加：

\`\`\`
[DINGTALK_VIDEO]{"path":"<本地视频路径>"}[/DINGTALK_VIDEO]
\`\`\`

**支持格式**：mp4（最大 20MB）

**重要**：
- 视频大小不得超过 20MB，超过限制时告知用户
- 仅支持 mp4 格式
- 系统会自动提取视频时长、分辨率并生成封面

### 三、音频分享

**何时分享音频**：
- ✅ 用户明确要求**分享、发送、上传**音频/语音文件时
- ❌ 仅生成音频保存到本地时，**不需要**分享

**音频标记格式**：
当需要分享音频时，在回复**末尾**添加：

\`\`\`
[DINGTALK_AUDIO]{"path":"<本地音频路径>"}[/DINGTALK_AUDIO]
\`\`\`

**支持格式**：ogg、amr（最大 20MB）

**重要**：
- 音频大小不得超过 20MB，超过限制时告知用户
- 系统会自动提取音频时长

### 四、文件分享

**何时分享文件**：
- ✅ 用户明确要求**分享、发送、上传**文件时
- ❌ 仅生成文件保存到本地时，**不需要**分享

**文件标记格式**：
当需要分享文件时，在回复**末尾**添加：

\`\`\`
[DINGTALK_FILE]{"path":"<本地文件路径>","fileName":"<文件名>","fileType":"<扩展名>"}[/DINGTALK_FILE]
\`\`\`

**支持的文件类型**：几乎所有常见格式

**重要**：文件大小不得超过 20MB，超过限制时告知用户文件过大。`;
}

// ---------- 2. 本地图片路径识别与上传 ----------
const LOCAL_IMAGE_RE =
  /!\[([^\]]*)\]\(((?:file:\/\/\/|MEDIA:|attachment:\/\/\/)[^)]+|\/(?:tmp|var|private|Users|home|root)[^)]+|[A-Za-z]:[\\/ ][^)]+)\)/g;
const BARE_IMAGE_PATH_RE =
  /`?((?:\/(?:tmp|var|private|Users|home|root)\/[^\s`'",)]+|[A-Za-z]:[\\/][^\s`'",)]+)\.(?:png|jpg|jpeg|gif|bmp|webp))`?/gi;

function toLocalPath(raw: string): string {
  let p = raw;
  if (p.startsWith('file://')) p = p.replace('file://', '');
  else if (p.startsWith('MEDIA:')) p = p.replace('MEDIA:', '');
  else if (p.startsWith('attachment://')) p = p.replace('attachment://', '');
  try {
    p = decodeURIComponent(p);
  } catch {
    // ignore
  }
  return p;
}

async function uploadMediaToDingTalk(
  filePath: string,
  mediaType: 'image' | 'file' | 'video' | 'voice',
  oapiToken: string,
  maxSize: number = 20 * 1024 * 1024,
  log?: any,
): Promise<string | null> {
  try {
    const fs = await import('fs');
    const path = await import('path');
    const FormData = (await import('form-data')).default;
    const absPath = toLocalPath(filePath);
    if (!fs.existsSync(absPath)) {
      log?.warn?.(`[DingTalk][${mediaType}] 文件不存在: ${absPath}`);
      return null;
    }
    const stats = fs.statSync(absPath);
    if (stats.size > maxSize) {
      log?.warn?.(`[DingTalk][${mediaType}] 文件过大: ${absPath}`);
      return null;
    }
    const form = new FormData();
    form.append('media', fs.createReadStream(absPath), {
      filename: path.basename(absPath),
      contentType: mediaType === 'image' ? 'image/jpeg' : 'application/octet-stream',
    });
    const resp = await axios.post(
      `https://oapi.dingtalk.com/media/upload?access_token=${oapiToken}&type=${mediaType}`,
      form,
      { headers: form.getHeaders(), timeout: 60_000 },
    );
    return resp.data?.media_id ?? null;
  } catch (err: any) {
    log?.error?.(`[DingTalk][${mediaType}] 上传失败: ${err.message}`);
    return null;
  }
}

/** 扫描内容中的本地图片路径，上传到钉钉并替换为 media_id */
export async function processLocalImages(
  content: string,
  oapiToken: string | null,
  log?: any,
): Promise<string> {
  if (!oapiToken) {
    log?.warn?.(`[DingTalk][Media] 无 oapiToken，跳过图片后处理`);
    return content;
  }
  let result = content;
  const mdMatches = [...content.matchAll(LOCAL_IMAGE_RE)];
  for (const match of mdMatches) {
    const [fullMatch, alt, rawPath] = match;
    const cleanPath = rawPath.replace(/\\ /g, ' ');
    const mediaId = await uploadMediaToDingTalk(cleanPath, 'image', oapiToken, 20 * 1024 * 1024, log);
    if (mediaId) result = result.replace(fullMatch, `![${alt}](${mediaId})`);
  }
  const bareMatches = [...result.matchAll(BARE_IMAGE_PATH_RE)];
  const newBareMatches = bareMatches.filter((m) => {
    const idx = m.index!;
    return !result.slice(Math.max(0, idx - 10), idx).includes('](');
  });
  for (const match of newBareMatches.reverse()) {
    const [fullMatch, rawPath] = match;
    const mediaId = await uploadMediaToDingTalk(rawPath, 'image', oapiToken, 20 * 1024 * 1024, log);
    if (mediaId) {
      const replacement = `![](${mediaId})`;
      result = result.slice(0, match.index!) + result.slice(match.index!).replace(fullMatch, replacement);
    }
  }
  return result;
}

// ---------- 3. DINGTALK_* 标记：文件 / 视频 / 音频 ----------
const MAX_VIDEO_SIZE = 20 * 1024 * 1024;
const MAX_FILE_SIZE = 20 * 1024 * 1024;
const DINGTALK_API = 'https://api.dingtalk.com';
const AUDIO_EXTENSIONS = ['mp3', 'wav', 'amr', 'ogg', 'aac', 'flac', 'm4a'];

interface VideoInfo {
  path: string;
}
interface VideoMetadata {
  duration: number;
  width: number;
  height: number;
}
interface FileInfo {
  path: string;
  fileName: string;
  fileType: string;
}
interface AudioInfo {
  path: string;
}

function isAudioFile(fileType: string): boolean {
  return AUDIO_EXTENSIONS.includes(fileType.toLowerCase());
}

async function extractVideoMetadata(filePath: string, log?: any): Promise<VideoMetadata | null> {
  try {
    const ffmpeg = require('fluent-ffmpeg');
    const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path;
    ffmpeg.setFfmpegPath(ffmpegPath);
    return new Promise((resolve, reject) => {
      ffmpeg.ffprobe(filePath, (err: any, metadata: any) => {
        if (err) return reject(err);
        const videoStream = metadata.streams?.find((s: any) => s.codec_type === 'video');
        if (!videoStream) return resolve(null);
        resolve({
          duration: Math.floor(metadata.format?.duration || 0),
          width: videoStream.width || 0,
          height: videoStream.height || 0,
        });
      });
    });
  } catch (err: any) {
    log?.error?.(`[DingTalk][Video] ffprobe 失败: ${err.message}`);
    return null;
  }
}

async function extractVideoThumbnail(videoPath: string, outputPath: string, log?: any): Promise<string | null> {
  try {
    const ffmpeg = require('fluent-ffmpeg');
    const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path;
    const path = await import('path');
    ffmpeg.setFfmpegPath(ffmpegPath);
    return new Promise((resolve, reject) => {
      ffmpeg(videoPath)
        .screenshots({
          count: 1,
          folder: path.dirname(outputPath),
          filename: path.basename(outputPath),
          timemarks: ['1'],
          size: '?x360',
        })
        .on('end', () => resolve(outputPath))
        .on('error', reject);
    });
  } catch (err: any) {
    log?.error?.(`[DingTalk][Video] 封面生成失败: ${(err as Error).message}`);
    return null;
  }
}

async function sendVideoMessagePassive(
  sessionWebhook: string,
  videoInfo: VideoInfo,
  videoMediaId: string,
  picMediaId: string,
  metadata: VideoMetadata,
  oapiToken: string,
  log?: any,
): Promise<void> {
  try {
    const payload = {
      msgtype: 'video',
      video: {
        duration: metadata.duration.toString(),
        videoMediaId,
        videoType: 'mp4',
        picMediaId,
      },
    };
    await axios.post(sessionWebhook, payload, {
      headers: { 'x-acs-dingtalk-access-token': oapiToken, 'Content-Type': 'application/json' },
      timeout: 10_000,
    });
  } catch (err: any) {
    log?.error?.(`[DingTalk][Video] 发送失败: ${(err as Error).message}`);
  }
}

async function sendVideoProactive(
  config: any,
  target: MediaFormatTarget,
  videoMediaId: string,
  picMediaId: string,
  metadata: VideoMetadata,
  log: any,
  getAccessToken: GetAccessToken,
): Promise<void> {
  try {
    const token = await getAccessToken(config);
    const msgParam = { duration: metadata.duration.toString(), videoMediaId, videoType: 'mp4', picMediaId };
    const body: any = { robotCode: config.clientId, msgKey: 'sampleVideo', msgParam: JSON.stringify(msgParam) };
    if (target.type === 'group') {
      body.openConversationId = target.openConversationId;
      await axios.post(`${DINGTALK_API}/v1.0/robot/groupMessages/send`, body, {
        headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
        timeout: 10_000,
      });
    } else {
      body.userIds = [target.userId];
      await axios.post(`${DINGTALK_API}/v1.0/robot/oToMessages/batchSend`, body, {
        headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
        timeout: 10_000,
      });
    }
  } catch (err: any) {
    log?.error?.(`[DingTalk][Video][Proactive] 发送失败: ${(err as Error).message}`);
  }
}

export async function processVideoMarkers(
  content: string,
  sessionWebhook: string,
  config: any,
  oapiToken: string | null,
  log?: any,
  useProactiveApi: boolean = false,
  target?: MediaFormatTarget,
  getAccessToken?: GetAccessToken,
): Promise<string> {
  if (!oapiToken) return content;
  const fs = await import('fs');
  const path = await import('path');
  const os = await import('os');
  const matches = [...content.matchAll(VIDEO_MARKER_PATTERN)];
  const videoInfos: VideoInfo[] = [];
  const invalidVideos: string[] = [];
  for (const match of matches) {
    try {
      const videoInfo = JSON.parse(match[1]) as VideoInfo;
      if (videoInfo.path && fs.existsSync(videoInfo.path)) videoInfos.push(videoInfo);
      else invalidVideos.push(videoInfo.path || '');
    } catch {
      // skip
    }
  }
  if (videoInfos.length === 0 && invalidVideos.length === 0) {
    return content.replace(VIDEO_MARKER_PATTERN, '').trim();
  }
  let cleanedContent = content.replace(VIDEO_MARKER_PATTERN, '').trim();
  const statusMessages: string[] = invalidVideos.map((p) => `⚠️ 视频文件不存在: ${path.basename(p)}`);
  for (const videoInfo of videoInfos) {
    const fileName = path.basename(videoInfo.path);
    let thumbnailPath = '';
    try {
      const metadata = await extractVideoMetadata(videoInfo.path, log);
      if (!metadata) {
        statusMessages.push(`⚠️ 视频处理失败: ${fileName}`);
        continue;
      }
      thumbnailPath = path.join(os.tmpdir(), `thumbnail_${Date.now()}.jpg`);
      const thumbnail = await extractVideoThumbnail(videoInfo.path, thumbnailPath, log);
      if (!thumbnail) {
        statusMessages.push(`⚠️ 视频处理失败: ${fileName}`);
        continue;
      }
      const videoMediaId = await uploadMediaToDingTalk(videoInfo.path, 'video', oapiToken, MAX_VIDEO_SIZE, log);
      if (!videoMediaId) {
        statusMessages.push(`⚠️ 视频上传失败: ${fileName}`);
        continue;
      }
      const picMediaId = await uploadMediaToDingTalk(thumbnailPath, 'image', oapiToken, 20 * 1024 * 1024, log);
      if (!picMediaId) {
        statusMessages.push(`⚠️ 视频封面上传失败: ${fileName}`);
        continue;
      }
      if (useProactiveApi && target && getAccessToken) {
        await sendVideoProactive(config, target, videoMediaId, picMediaId, metadata, log, getAccessToken);
      } else {
        await sendVideoMessagePassive(sessionWebhook, videoInfo, videoMediaId, picMediaId, metadata, oapiToken, log);
      }
      statusMessages.push(`✅ 视频已发送: ${fileName}`);
    } catch (err: any) {
      statusMessages.push(`⚠️ 视频处理异常: ${fileName}`);
    } finally {
      if (thumbnailPath) try { fs.unlinkSync(thumbnailPath); } catch { /* ignore */ }
    }
  }
  if (statusMessages.length > 0) {
    cleanedContent = cleanedContent ? `${cleanedContent}\n\n${statusMessages.join('\n')}` : statusMessages.join('\n');
  }
  return cleanedContent;
}

function extractFileMarkers(content: string): { cleanedContent: string; fileInfos: FileInfo[] } {
  const fileInfos: FileInfo[] = [];
  const matches = [...content.matchAll(FILE_MARKER_PATTERN)];
  for (const match of matches) {
    try {
      const fileInfo = JSON.parse(match[1]) as FileInfo;
      if (fileInfo.path && fileInfo.fileName) fileInfos.push(fileInfo);
    } catch {
      // skip
    }
  }
  return { cleanedContent: content.replace(FILE_MARKER_PATTERN, '').trim(), fileInfos };
}

async function sendFileMessagePassive(
  sessionWebhook: string,
  fileInfo: FileInfo,
  mediaId: string,
  oapiToken: string,
): Promise<void> {
  try {
    await axios.post(
      sessionWebhook,
      { msgtype: 'file', file: { mediaId, fileName: fileInfo.fileName, fileType: fileInfo.fileType } },
      { headers: { 'x-acs-dingtalk-access-token': oapiToken, 'Content-Type': 'application/json' }, timeout: 10_000 },
    );
  } catch {
    // ignore
  }
}

async function sendAudioMessagePassive(
  sessionWebhook: string,
  fileInfo: FileInfo,
  mediaId: string,
  oapiToken: string,
): Promise<void> {
  try {
    await axios.post(
      sessionWebhook,
      { msgtype: 'voice', voice: { mediaId, duration: '60000' } },
      { headers: { 'x-acs-dingtalk-access-token': oapiToken, 'Content-Type': 'application/json' }, timeout: 10_000 },
    );
  } catch {
    // ignore
  }
}

async function sendFileProactive(
  config: any,
  target: MediaFormatTarget,
  fileInfo: FileInfo,
  mediaId: string,
  log: any,
  getAccessToken: GetAccessToken,
): Promise<void> {
  try {
    const token = await getAccessToken(config);
    const body: any = {
      robotCode: config.clientId,
      msgKey: 'sampleFile',
      msgParam: JSON.stringify({ mediaId, fileName: fileInfo.fileName, fileType: fileInfo.fileType }),
    };
    if (target.type === 'group') {
      body.openConversationId = target.openConversationId;
      await axios.post(`${DINGTALK_API}/v1.0/robot/groupMessages/send`, body, {
        headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
        timeout: 10_000,
      });
    } else {
      body.userIds = [target.userId];
      await axios.post(`${DINGTALK_API}/v1.0/robot/oToMessages/batchSend`, body, {
        headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
        timeout: 10_000,
      });
    }
  } catch (err: any) {
    log?.error?.(`[DingTalk][File][Proactive] 发送失败: ${(err as Error).message}`);
  }
}

async function sendAudioProactive(
  config: any,
  target: MediaFormatTarget,
  fileInfo: FileInfo,
  mediaId: string,
  log: any,
  getAccessToken: GetAccessToken,
): Promise<void> {
  try {
    const token = await getAccessToken(config);
    const body: any = {
      robotCode: config.clientId,
      msgKey: 'sampleAudio',
      msgParam: JSON.stringify({ mediaId, duration: '60000' }),
    };
    if (target.type === 'group') {
      body.openConversationId = target.openConversationId;
      await axios.post(`${DINGTALK_API}/v1.0/robot/groupMessages/send`, body, {
        headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
        timeout: 10_000,
      });
    } else {
      body.userIds = [target.userId];
      await axios.post(`${DINGTALK_API}/v1.0/robot/oToMessages/batchSend`, body, {
        headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
        timeout: 10_000,
      });
    }
  } catch (err: any) {
    log?.error?.(`[DingTalk][Audio][Proactive] 发送失败: ${(err as Error).message}`);
  }
}

export async function processFileMarkers(
  content: string,
  sessionWebhook: string,
  config: any,
  oapiToken: string | null,
  log?: any,
  useProactiveApi: boolean = false,
  target?: MediaFormatTarget,
  getAccessToken?: GetAccessToken,
): Promise<string> {
  if (!oapiToken) return content;
  const { cleanedContent, fileInfos } = extractFileMarkers(content);
  if (fileInfos.length === 0) return cleanedContent;
  const fs = await import('fs');
  const statusMessages: string[] = [];
  for (const fileInfo of fileInfos) {
    const absPath = toLocalPath(fileInfo.path);
    if (!fs.existsSync(absPath)) {
      statusMessages.push(`⚠️ 文件不存在: ${fileInfo.fileName}`);
      continue;
    }
    const stats = fs.statSync(absPath);
    if (stats.size > MAX_FILE_SIZE) {
      statusMessages.push(`⚠️ 文件过大无法发送: ${fileInfo.fileName}`);
      continue;
    }
    if (isAudioFile(fileInfo.fileType)) {
      const mediaId = await uploadMediaToDingTalk(fileInfo.path, 'voice', oapiToken, MAX_FILE_SIZE, log);
      if (mediaId) {
        if (useProactiveApi && target && getAccessToken) {
          await sendAudioProactive(config, target, fileInfo, mediaId, log, getAccessToken);
        } else {
          await sendAudioMessagePassive(sessionWebhook, fileInfo, mediaId, oapiToken);
        }
        statusMessages.push(`✅ 音频已发送: ${fileInfo.fileName}`);
      } else statusMessages.push(`⚠️ 音频上传失败: ${fileInfo.fileName}`);
    } else {
      const mediaId = await uploadMediaToDingTalk(fileInfo.path, 'file', oapiToken, MAX_FILE_SIZE, log);
      if (mediaId) {
        if (useProactiveApi && target && getAccessToken) {
          await sendFileProactive(config, target, fileInfo, mediaId, log, getAccessToken);
        } else {
          await sendFileMessagePassive(sessionWebhook, fileInfo, mediaId, oapiToken);
        }
        statusMessages.push(`✅ 文件已发送: ${fileInfo.fileName}`);
      } else statusMessages.push(`⚠️ 文件上传失败: ${fileInfo.fileName}`);
    }
  }
  if (statusMessages.length > 0) {
    return cleanedContent ? `${cleanedContent}\n\n${statusMessages.join('\n')}` : statusMessages.join('\n');
  }
  return cleanedContent;
}

export async function processAudioMarkers(
  content: string,
  sessionWebhook: string,
  config: any,
  oapiToken: string | null,
  log?: any,
  useProactiveApi: boolean = false,
  target?: MediaFormatTarget,
  getAccessToken?: GetAccessToken,
): Promise<string> {
  if (!oapiToken) return content;
  const fs = await import('fs');
  const path = await import('path');
  const matches = [...content.matchAll(AUDIO_MARKER_PATTERN)];
  const audioInfos: AudioInfo[] = [];
  const invalidAudios: string[] = [];
  for (const match of matches) {
    try {
      const audioInfo = JSON.parse(match[1]) as AudioInfo;
      if (audioInfo.path && fs.existsSync(audioInfo.path)) audioInfos.push(audioInfo);
      else invalidAudios.push(audioInfo.path || '');
    } catch {
      // skip
    }
  }
  if (audioInfos.length === 0 && invalidAudios.length === 0) {
    return content.replace(AUDIO_MARKER_PATTERN, '').trim();
  }
  let cleanedContent = content.replace(AUDIO_MARKER_PATTERN, '').trim();
  const statusMessages: string[] = invalidAudios.map((p) => `⚠️ 音频文件不存在: ${path.basename(p)}`);
  for (const audioInfo of audioInfos) {
    const fileName = path.basename(audioInfo.path);
    try {
      const ext = path.extname(audioInfo.path).slice(1).toLowerCase();
      const fileInfo: FileInfo = { path: audioInfo.path, fileName, fileType: ext };
      const mediaId = await uploadMediaToDingTalk(audioInfo.path, 'voice', oapiToken, 20 * 1024 * 1024, log);
      if (!mediaId) {
        statusMessages.push(`⚠️ 音频上传失败: ${fileName}`);
        continue;
      }
      if (useProactiveApi && target && getAccessToken) {
        await sendAudioProactive(config, target, fileInfo, mediaId, log, getAccessToken);
      } else {
        await sendAudioMessagePassive(sessionWebhook, fileInfo, mediaId, oapiToken);
      }
      statusMessages.push(`✅ 音频已发送: ${fileName}`);
    } catch {
      statusMessages.push(`⚠️ 音频处理异常: ${fileName}`);
    }
  }
  if (statusMessages.length > 0) {
    cleanedContent = cleanedContent ? `${cleanedContent}\n\n${statusMessages.join('\n')}` : statusMessages.join('\n');
  }
  return cleanedContent;
}
