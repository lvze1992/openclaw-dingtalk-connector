/**
 * DingTalk Channel Plugin for Moltbot
 *
 * 通过钉钉 Stream 模式连接，支持 AI Card 流式响应。
 * 完整接入 Moltbot 消息处理管道。
 *
 * 运行脉络：常量/Session/Token/配置 → 媒体下载 → [特殊格式见 media-formats.ts] →
 * AI Card 流式 → Gateway SSE → 消息解析 → 主动发送 API → handleDingTalkMessage → 插件定义/注册
 */

import { DWClient, TOPIC_ROBOT } from 'dingtalk-stream';
import axios from 'axios';
import type { ClawdbotPluginApi, PluginRuntime, ClawdbotConfig } from 'clawdbot/plugin-sdk';
import {
  buildMediaSystemPrompt,
  processLocalImages,
  processFileMarkers,
  processVideoMarkers,
  processAudioMarkers,
  FILE_MARKER_PATTERN,
  VIDEO_MARKER_PATTERN,
  AUDIO_MARKER_PATTERN,
} from './media-formats';
import { getSessionKey, isMessageProcessed, markMessageProcessed, isNewSessionCommand } from './session';
import { getAccessToken, getOapiAccessToken } from './tokens';
import { sendMessage, sendTextMessage, sendMarkdownMessage } from './text-card';
import { downloadMediaFromDingTalk } from './media-download';
import {
  createAICard,
  streamAICard,
  finishAICard,
  createAICardForTarget,
  type AICardTarget,
} from './ai-card';

// ============ 常量 ============

const DINGTALK_API = 'https://api.dingtalk.com';

export const id = 'dingtalk-connector';

let runtime: PluginRuntime | null = null;

function getRuntime(): PluginRuntime {
  if (!runtime) throw new Error('DingTalk runtime not initialized');
  return runtime;
}

// ============ 配置工具 ============

function getConfig(cfg: ClawdbotConfig) {
  return (cfg?.channels as any)?.['dingtalk-connector'] || {};
}

function isConfigured(cfg: ClawdbotConfig): boolean {
  const config = getConfig(cfg);
  return Boolean(config.clientId && config.clientSecret);
}

/** 判断 senderId 是否在 allowlist 中（支持裸 ID、user:xxx、dingtalk:xxx） */
function isSenderInAllowlist(senderId: string, allowList: string[]): boolean {
  const normalized = String(senderId).trim();
  return allowList.some((entry: string) => {
    const e = String(entry).trim();
    return e === normalized || e === `user:${normalized}` || e === `dingtalk:${normalized}`;
  });
}

// AI Card 长任务阈值（毫秒）
const LONG_TASK_NOTIFY_THRESHOLD_MS = 60_000;

/**
 * 如果 AI Card 处理时间超过阈值，在结束后额外通知一次需求发起人。
 */
async function notifyIfLongAICardTask(params: {
  startedAt: number;
  isDirect: boolean;
  dingtalkConfig: any;
  sessionWebhook: string;
  senderId: string;
  log?: any;
}): Promise<void> {
  const { startedAt, isDirect, dingtalkConfig, sessionWebhook, senderId, log } = params;
  const elapsedMs = Date.now() - startedAt;

  if (elapsedMs < LONG_TASK_NOTIFY_THRESHOLD_MS) {
    return;
  }

  const elapsedSeconds = Math.round(elapsedMs / 1000);
  log?.info?.(
    `[DingTalk] 长任务完成通知: sender=${senderId}, elapsed=${elapsedMs}ms (~${elapsedSeconds}s)`,
  );

  await sendMessage(
    dingtalkConfig,
    sessionWebhook,
    `✅ 您刚才发起的任务已处理完成，耗时约 ${elapsedSeconds} 秒。`,
    {
      atUserId: !isDirect ? senderId : null,
    },
  );
}

// ============ Gateway SSE Streaming ============

interface GatewayOptions {
  userContent: string;
  systemPrompts: string[];
  sessionKey: string;
  gatewayAuth?: string;  // token 或 password，都用 Bearer 格式
  log?: any;
}

async function* streamFromGateway(options: GatewayOptions): AsyncGenerator<string, void, unknown> {
  const { userContent, systemPrompts, sessionKey, gatewayAuth, log } = options;
  const rt = getRuntime();
  const gatewayUrl = `http://127.0.0.1:${rt.gateway?.port || 18789}/v1/chat/completions`;

  const messages: any[] = [];
  for (const prompt of systemPrompts) {
    messages.push({ role: 'system', content: prompt });
  }
  messages.push({ role: 'user', content: userContent });

  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (gatewayAuth) {
    headers['Authorization'] = `Bearer ${gatewayAuth}`;
  }

  log?.info?.(`[DingTalk][Gateway] POST ${gatewayUrl}, session=${sessionKey}, messages=${messages.length}`);

  const response = await fetch(gatewayUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      model: 'default',
      messages,
      stream: true,
      user: sessionKey,  // 用于 session 持久化
    }),
  });

  log?.info?.(`[DingTalk][Gateway] 响应 status=${response.status}, ok=${response.ok}, hasBody=${!!response.body}`);

  if (!response.ok || !response.body) {
    const errText = response.body ? await response.text() : '(no body)';
    log?.error?.(`[DingTalk][Gateway] 错误响应: ${errText}`);
    throw new Error(`Gateway error: ${response.status} - ${errText}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      if (!line.startsWith('data: ')) continue;
      const data = line.slice(6).trim();
      if (data === '[DONE]') return;

      try {
        const chunk = JSON.parse(data);
        const content = chunk.choices?.[0]?.delta?.content;
        if (content) yield content;
      } catch {}
    }
  }
}

// ============ 消息处理 ============

/** 将钉钉 atUsers 补回为文首说明，避免 @ 提及被从 content 中 strip 后模型看不到谁被@。过滤掉机器人自己，且只展示有 staffId 的记录。 */
function prependAtUsersHint(text: string, data: any): string {
  const atUsers = data?.atUsers;
  if (!Array.isArray(atUsers) || atUsers.length === 0) return text;
  const chatbotUserId = data?.chatbotUserId;
  const filtered = atUsers.filter(
    (u: any) =>
      typeof u?.staffId === 'string' &&
      u.staffId.length > 0 &&
      (!chatbotUserId || u.dingtalkId !== chatbotUserId),
  );
  if (filtered.length === 0) return text;
  const labels = filtered
    .map((u: any) => u.nick || u.name || u.staffId || '')
    .filter(Boolean);
  if (labels.length === 0) return text;
  return `【消息中@了以下用户（按顺序）: ${labels.join('、')}】\n${text}`;
}

async function extractMessageContent(
  data: any,
  config: any,
  log?: any,
): Promise<{ text: string; messageType: string }> {
  const msgtype = data.msgtype || 'text';
  switch (msgtype) {
    case 'text': {
      let text = data.text?.content?.trim() || '';
      // {"senderPlatform":"Mac","conversationId":"cid5tkiCHmOJDU5SKCJQ6LduQ==","atUsers":[{"unionId":"jTrnBfnLEJdVzXd3sbz9MwiEiE","dingtalkId":"$:LWCP_v1:$WzRkWxokqn6fLW59ngC/nA==","staffId":"06126959521181497"},{"dingtalkId":"$:LWCP_v1:$+K87Fw8SS0jZtnlIoQHtg4gP6LDOe2jf"}],"chatbotCorpId":"dingb3fcb372fae00784ffe93478753d9884","chatbotUserId":"$:LWCP_v1:$+K87Fw8SS0jZtnlIoQHtg4gP6LDOe2jf","openThreadId":"cid5tkiCHmOJDU5SKCJQ6LduQ==","msgId":"msgRJX2OofHcT+WqWiGaijkXw==","senderNick":"吕泽","isAdmin":true,"senderStaffId":"0948351305695176","sessionWebhookExpiredTime":1772382335198,"createAt":1772376934909,"senderCorpId":"dingb3fcb372fae00784ffe93478753d9884","conversationType":"2","senderId":"$:LWCP_v1:$EHg5XnVjrDCN9rSmgbs/HA==","conversationTitle":"机器人测试","isInAtList":true,"sessionWebhook":"https://oapi.dingtalk.com/robot/sendBySession?session=1d6cd44b6b8c154bb44d654a6039fc4a","senderUnionId":"rkq8m6nIlrmaPC3HLyECZAiEiE","text":{"content":" 给 发个消息“你好”"},"robotCode":"dingpul2ts6gnizcomqo","msgtype":"text"}
      text = prependAtUsersHint(text, data);
      return { text, messageType: 'text' };
    }
    case 'richText': {
      const content = data.content ?? {};
      const parts = content.richText || [];
      //{"senderPlatform":"Mac","conversationId":"cid5tkiCHmOJDU5SKCJQ6LduQ==","atUsers":[{"unionId":"jTrnBfnLEJdVzXd3sbz9MwiEiE","dingtalkId":"$:LWCP_v1:$WzRkWxokqn6fLW59ngC/nA==","staffId":"06126959521181497"},{"dingtalkId":"$:LWCP_v1:$+K87Fw8SS0jZtnlIoQHtg4gP6LDOe2jf"}],"chatbotCorpId":"dingb3fcb372fae00784ffe93478753d9884","chatbotUserId":"$:LWCP_v1:$+K87Fw8SS0jZtnlIoQHtg4gP6LDOe2jf","openThreadId":"cid5tkiCHmOJDU5SKCJQ6LduQ==","msgId":"msgUJTK/cnRC6JpQzFM+18XTA==","senderNick":"吕泽","isAdmin":true,"senderStaffId":"0948351305695176","sessionWebhookExpiredTime":1772382687862,"createAt":1772377287698,"content":{"richText":[{"text":"@小白"},{"text":"给"},{"text":"@释心"},{"text":"发个消息“"},{"text":"你12好"},{"text":"”"}]},"senderCorpId":"dingb3fcb372fae00784ffe93478753d9884","conversationType":"2","senderId":"$:LWCP_v1:$EHg5XnVjrDCN9rSmgbs/HA==","conversationTitle":"机器人测试","isInAtList":true,"sessionWebhook":"https://oapi.dingtalk.com/robot/sendBySession?session=1d6cd44b6b8c154bb44d654a6039fc4a","senderUnionId":"rkq8m6nIlrmaPC3HLyECZAiEiE","robotCode":"dingpul2ts6gnizcomqo","msgtype":"richText"}
      // 钉钉富文本 part 可能是 { type: 'text', text }、{ type: 'mention', ... } 或 { type: 'paragraph', content: [...] } 等
      // 实际回调格式（粘贴在此）:
      // 
      let text = parts
        .map((p: any) => {
          if (p?.type === 'mention' || p?.type === 'at') return `@${p.nick || p.name || p.staffId || p.dingtalkId || '用户'}`;
          if (typeof p?.text === 'string') return p.text;
          if (typeof p?.content === 'string') return p.content;
          if (Array.isArray(p?.content)) {
            return p.content.map((c: any) => c?.text ?? c?.content ?? '').join('');
          }
          return '';
        })
        .filter(Boolean)
        .join('\n');
      if (!text.trim() && typeof content.text === 'string') text = content.text;
      text = text.trim() || '[富文本消息]';
      text = prependAtUsersHint(text, data);
      return { text, messageType: 'richText' };
    }
    case 'picture': {
      // 钉钉图片消息下载码字段为 pictureDownloadCode（Stream 回调里为 content.pictureDownloadCode）
      // 按官方文档，真正用于文件下载的是 downloadCode，这里优先使用 downloadCode
      const downloadCode = data.content?.downloadCode ?? data.content?.pictureDownloadCode;
      const fileName = data.content?.fileName || 'image.jpg';
      if (downloadCode && config?.clientId) {
        log?.info?.(`[DingTalk][Message] 检测到图片消息，开始下载...`);
        const localPath = await downloadMediaFromDingTalk(config, downloadCode, fileName, log);
        if (localPath) {
          return { text: `用户发送了图片: ${localPath}`, messageType: 'picture' };
        }
      }
      return { text: '[图片下载失败]', messageType: 'picture' };
    }
    case 'audio':
      return { text: data.content?.recognition || '[语音消息]', messageType: 'audio' };
    case 'video':
      return { text: '[视频]', messageType: 'video' };
    case 'file': {
      const downloadCode = data.content?.downloadCode;
      const fileName = data.content?.fileName || 'file';
      if (downloadCode && config?.clientId) {
        log?.info?.(`[DingTalk][Message] 检测到文件消息，开始下载...`);
        const localPath = await downloadMediaFromDingTalk(config, downloadCode, fileName, log);
        if (localPath) {
          return { text: `用户发送了文件: ${localPath}`, messageType: 'file' };
        }
      }
      return { text: `[文件: ${data.content?.fileName || '文件'}]`, messageType: 'file' };
    }
    default:
      return { text: data.text?.content?.trim() || `[${msgtype}消息]`, messageType: msgtype };
  }
}

// ============ 主动发送消息 API ============

/** 消息类型枚举 */
type DingTalkMsgType = 'text' | 'markdown' | 'link' | 'actionCard' | 'image';

/** 主动发送消息的结果 */
interface SendResult {
  ok: boolean;
  processQueryKey?: string;
  cardInstanceId?: string;  // AI Card 成功时返回
  error?: string;
  usedAICard?: boolean;  // 是否使用了 AI Card
}

/** 主动发送选项 */
interface ProactiveSendOptions {
  msgType?: DingTalkMsgType;
  title?: string;
  log?: any;
  useAICard?: boolean;  // 是否使用 AI Card，默认 true
  fallbackToNormal?: boolean;  // AI Card 失败时是否降级到普通消息，默认 true
}

/**
 * 主动创建并发送 AI Card（通用内部实现）
 * 复用 createAICardForTarget 并完整支持后处理
 * @param config 钉钉配置
 * @param target 投放目标（单聊或群聊）
 * @param content 消息内容
 * @param log 日志对象
 * @returns SendResult
 */
async function sendAICardInternal(
  config: any,
  target: AICardTarget,
  content: string,
  log?: any,
): Promise<SendResult> {
  const targetDesc = target.type === 'group'
    ? `群聊 ${target.openConversationId}`
    : `用户 ${target.userId}`;

  try {
    // 0. 获取 oapiToken 用于后处理
    const oapiToken = await getOapiAccessToken(config);

    // 1. 后处理01：上传本地图片到钉钉，替换路径为 media_id
    let processedContent = content;
    if (oapiToken) {
      log?.info?.(`[DingTalk][AICard][Proactive] 开始图片后处理`);
      processedContent = await processLocalImages(content, oapiToken, log);
    } else {
      log?.warn?.(`[DingTalk][AICard][Proactive] 无法获取 oapiToken，跳过媒体后处理`);
    }

    // 2. 后处理02：提取视频标记并发送视频消息
    log?.info?.(`[DingTalk][Video][Proactive] 开始视频后处理`);
    processedContent = await processVideoMarkers(processedContent, '', config, oapiToken, log, true, target, getAccessToken);

    // 3. 后处理03：提取音频标记并发送音频消息（使用主动消息 API）
    log?.info?.(`[DingTalk][Audio][Proactive] 开始音频后处理`);
    processedContent = await processAudioMarkers(processedContent, '', config, oapiToken, log, true, target, getAccessToken);

    // 4. 后处理04：提取文件标记并发送独立文件消息（使用主动消息 API）
    log?.info?.(`[DingTalk][File][Proactive] 开始文件后处理`);
    processedContent = await processFileMarkers(processedContent, '', config, oapiToken, log, true, target, getAccessToken);

    // 5. 检查处理后的内容是否为空（纯文件/视频/音频消息场景）
    //    如果内容只包含文件/视频/音频标记，处理后会变成空字符串，此时跳过创建空白 AI Card
    const trimmedContent = processedContent.trim();
    if (!trimmedContent) {
      log?.info?.(`[DingTalk][AICard][Proactive] 处理后内容为空（纯文件/视频消息），跳过创建 AI Card`);
      return { ok: true, usedAICard: false };
    }

    // useAIStreamCard=false 时跳过 AI Card，直接用普通消息
    const useAIStreamCard = config.useAIStreamCard !== false;
    if (!useAIStreamCard) {
      log?.info?.(`[DingTalk][AICard][Proactive] useAIStreamCard=false，使用普通消息`);
      if (target.type === 'user') {
        return sendNormalToUser(config, target.userId, trimmedContent, { msgType: 'markdown', log });
      }
      return sendNormalToGroup(config, target.openConversationId!, trimmedContent, { msgType: 'markdown', log });
    }

    // 5. 创建卡片（复用通用函数）
    const card = await createAICardForTarget(config, target, log);
    if (!card) {
      return { ok: false, error: 'Failed to create AI Card', usedAICard: false };
    }

    // 6. 使用 finishAICard 设置内容
    await finishAICard(card, processedContent, log);

    log?.info?.(`[DingTalk][AICard][Proactive] AI Card 发送成功: ${targetDesc}, cardInstanceId=${card.cardInstanceId}`);
    return { ok: true, cardInstanceId: card.cardInstanceId, usedAICard: true };

  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard][Proactive] AI Card 发送失败 (${targetDesc}): ${err.message}`);
    if (err.response) {
      log?.error?.(`[DingTalk][AICard][Proactive] 错误响应: status=${err.response.status} data=${JSON.stringify(err.response.data)}`);
    }
    return { ok: false, error: err.response?.data?.message || err.message, usedAICard: false };
  }
}

/**
 * 主动发送 AI Card 到单聊用户
 */
async function sendAICardToUser(
  config: any,
  userId: string,
  content: string,
  log?: any,
): Promise<SendResult> {
  return sendAICardInternal(config, { type: 'user', userId }, content, log);
}

/**
 * 主动发送 AI Card 到群聊
 */
async function sendAICardToGroup(
  config: any,
  openConversationId: string,
  content: string,
  log?: any,
): Promise<SendResult> {
  return sendAICardInternal(config, { type: 'group', openConversationId }, content, log);
}

/**
 * 构建普通消息的 msgKey 和 msgParam
 * 提取公共逻辑，供 sendNormalToUser 和 sendNormalToGroup 复用
 */
function buildMsgPayload(
  msgType: DingTalkMsgType,
  content: string,
  title?: string,
): { msgKey: string; msgParam: Record<string, any> } | { error: string } {
  switch (msgType) {
    case 'markdown':
      return {
        msgKey: 'sampleMarkdown',
        msgParam: {
          title: title || content.split('\n')[0].replace(/^[#*\s\->]+/, '').slice(0, 20) || 'Message',
          text: content,
        },
      };
    case 'link':
      try {
        return {
          msgKey: 'sampleLink',
          msgParam: typeof content === 'string' ? JSON.parse(content) : content,
        };
      } catch {
        return { error: 'Invalid link message format, expected JSON' };
      }
    case 'actionCard':
      try {
        return {
          msgKey: 'sampleActionCard',
          msgParam: typeof content === 'string' ? JSON.parse(content) : content,
        };
      } catch {
        return { error: 'Invalid actionCard message format, expected JSON' };
      }
    case 'image':
      return {
        msgKey: 'sampleImageMsg',
        msgParam: { photoURL: content },
      };
    case 'text':
    default:
      return {
        msgKey: 'sampleText',
        msgParam: { content },
      };
  }
}

/**
 * 使用普通消息 API 发送单聊消息（降级方案）
 */
async function sendNormalToUser(
  config: any,
  userIds: string | string[],
  content: string,
  options: { msgType?: DingTalkMsgType; title?: string; log?: any } = {},
): Promise<SendResult> {
  const { msgType = 'text', title, log } = options;
  const userIdArray = Array.isArray(userIds) ? userIds : [userIds];

  // 构建消息参数
  const payload = buildMsgPayload(msgType, content, title);
  if ('error' in payload) {
    return { ok: false, error: payload.error, usedAICard: false };
  }

  try {
    const token = await getAccessToken(config);
    const body = {
      robotCode: config.clientId,
      userIds: userIdArray,
      msgKey: payload.msgKey,
      msgParam: JSON.stringify(payload.msgParam),
    };

    log?.info?.(`[DingTalk][Normal] 发送单聊消息: userIds=${userIdArray.join(',')}, msgType=${msgType}`);

    const resp = await axios.post(`${DINGTALK_API}/v1.0/robot/oToMessages/batchSend`, body, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
      timeout: 10_000,
    });

    if (resp.data?.processQueryKey) {
      log?.info?.(`[DingTalk][Normal] 发送成功: processQueryKey=${resp.data.processQueryKey}`);
      return { ok: true, processQueryKey: resp.data.processQueryKey, usedAICard: false };
    }

    log?.warn?.(`[DingTalk][Normal] 发送响应异常: ${JSON.stringify(resp.data)}`);
    return { ok: false, error: resp.data?.message || 'Unknown error', usedAICard: false };
  } catch (err: any) {
    const errMsg = err.response?.data?.message || err.message;
    log?.error?.(`[DingTalk][Normal] 发送失败: ${errMsg}`);
    return { ok: false, error: errMsg, usedAICard: false };
  }
}

/**
 * 使用普通消息 API 发送群聊消息（降级方案）
 */
async function sendNormalToGroup(
  config: any,
  openConversationId: string,
  content: string,
  options: { msgType?: DingTalkMsgType; title?: string; log?: any } = {},
): Promise<SendResult> {
  const { msgType = 'text', title, log } = options;

  // 构建消息参数
  const payload = buildMsgPayload(msgType, content, title);
  if ('error' in payload) {
    return { ok: false, error: payload.error, usedAICard: false };
  }

  try {
    const token = await getAccessToken(config);
    const body = {
      robotCode: config.clientId,
      openConversationId,
      msgKey: payload.msgKey,
      msgParam: JSON.stringify(payload.msgParam),
    };

    log?.info?.(`[DingTalk][Normal] 发送群聊消息: openConversationId=${openConversationId}, msgType=${msgType}`);

    const resp = await axios.post(`${DINGTALK_API}/v1.0/robot/groupMessages/send`, body, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
      timeout: 10_000,
    });

    if (resp.data?.processQueryKey) {
      log?.info?.(`[DingTalk][Normal] 发送成功: processQueryKey=${resp.data.processQueryKey}`);
      return { ok: true, processQueryKey: resp.data.processQueryKey, usedAICard: false };
    }

    log?.warn?.(`[DingTalk][Normal] 发送响应异常: ${JSON.stringify(resp.data)}`);
    return { ok: false, error: resp.data?.message || 'Unknown error', usedAICard: false };
  } catch (err: any) {
    const errMsg = err.response?.data?.message || err.message;
    log?.error?.(`[DingTalk][Normal] 发送失败: ${errMsg}`);
    return { ok: false, error: errMsg, usedAICard: false };
  }
}

/**
 * 主动发送单聊消息给指定用户
 * 默认使用 AI Card，失败时降级到普通消息
 * @param config 钉钉配置（需包含 clientId 和 clientSecret）
 * @param userIds 用户 ID 数组（staffId 或 unionId）
 * @param content 消息内容
 * @param options 可选配置
 */
async function sendToUser(
  config: any,
  userIds: string | string[],
  content: string,
  options: ProactiveSendOptions = {},
): Promise<SendResult> {
  const { log, useAICard = true, fallbackToNormal = true } = options;

  if (!config.clientId || !config.clientSecret) {
    return { ok: false, error: 'Missing clientId or clientSecret', usedAICard: false };
  }

  const userIdArray = Array.isArray(userIds) ? userIds : [userIds];
  if (userIdArray.length === 0) {
    return { ok: false, error: 'userIds cannot be empty', usedAICard: false };
  }

  // AI Card 只支持单个用户
  if (useAICard && userIdArray.length === 1) {
    log?.info?.(`[DingTalk][SendToUser] 尝试使用 AI Card 发送: userId=${userIdArray[0]}`);
    const cardResult = await sendAICardToUser(config, userIdArray[0], content, log);

    if (cardResult.ok) {
      return cardResult;
    }

    // AI Card 失败
    log?.warn?.(`[DingTalk][SendToUser] AI Card 发送失败: ${cardResult.error}`);

    if (!fallbackToNormal) {
      log?.error?.(`[DingTalk][SendToUser] 不降级到普通消息，返回错误`);
      return cardResult;
    }

    log?.info?.(`[DingTalk][SendToUser] 降级到普通消息发送`);
  } else if (useAICard && userIdArray.length > 1) {
    log?.info?.(`[DingTalk][SendToUser] 多用户发送不支持 AI Card，使用普通消息`);
  }

  // 使用普通消息
  return sendNormalToUser(config, userIdArray, content, options);
}

/**
 * 主动发送群聊消息到指定群
 * 默认使用 AI Card，失败时降级到普通消息
 * @param config 钉钉配置（需包含 clientId 和 clientSecret）
 * @param openConversationId 群会话 ID
 * @param content 消息内容
 * @param options 可选配置
 */
async function sendToGroup(
  config: any,
  openConversationId: string,
  content: string,
  options: ProactiveSendOptions = {},
): Promise<SendResult> {
  const { log, useAICard = true, fallbackToNormal = true } = options;

  if (!config.clientId || !config.clientSecret) {
    return { ok: false, error: 'Missing clientId or clientSecret', usedAICard: false };
  }

  if (!openConversationId) {
    return { ok: false, error: 'openConversationId cannot be empty', usedAICard: false };
  }

  // 尝试使用 AI Card
  if (useAICard) {
    log?.info?.(`[DingTalk][SendToGroup] 尝试使用 AI Card 发送: openConversationId=${openConversationId}`);
    const cardResult = await sendAICardToGroup(config, openConversationId, content, log);

    if (cardResult.ok) {
      return cardResult;
    }

    // AI Card 失败
    log?.warn?.(`[DingTalk][SendToGroup] AI Card 发送失败: ${cardResult.error}`);

    if (!fallbackToNormal) {
      log?.error?.(`[DingTalk][SendToGroup] 不降级到普通消息，返回错误`);
      return cardResult;
    }

    log?.info?.(`[DingTalk][SendToGroup] 降级到普通消息发送`);
  }

  // 使用普通消息
  return sendNormalToGroup(config, openConversationId, content, options);
}

/**
 * 智能发送消息
 * 默认使用 AI Card，失败时降级到普通消息
 * @param config 钉钉配置
 * @param target 目标：{ userId } 或 { openConversationId }
 * @param content 消息内容
 * @param options 可选配置
 */
async function sendProactive(
  config: any,
  target: { userId?: string; userIds?: string[]; openConversationId?: string },
  content: string,
  options: ProactiveSendOptions = {},
): Promise<SendResult> {
  // 自动检测是否使用 markdown（用于降级时）
  if (!options.msgType) {
    const hasMarkdown = /^[#*>-]|[*_`#\[\]]/.test(content) || content.includes('\n');
    if (hasMarkdown) {
      options.msgType = 'markdown';
    }
  }

  // 发送到用户
  if (target.userId || target.userIds) {
    const userIds = target.userIds || [target.userId!];
    return sendToUser(config, userIds, content, options);
  }

  // 发送到群
  if (target.openConversationId) {
    return sendToGroup(config, target.openConversationId, content, options);
  }

  return { ok: false, error: 'Must specify userId, userIds, or openConversationId', usedAICard: false };
}

// ============ 核心消息处理 (AI Card Streaming) ============

async function handleDingTalkMessage(params: {
  cfg: ClawdbotConfig;
  accountId: string;
  data: any;
  sessionWebhook: string;
  log?: any;
  dingtalkConfig: any;
}): Promise<void> {
  const { cfg, accountId, data, sessionWebhook, log, dingtalkConfig } = params;

  const isDirect = data.conversationType === '1';
  const senderId = data.senderStaffId || data.senderId;
  const senderName = data.senderNick || 'Unknown';

  // ===== Allowlist 检查（私聊 / 群聊共用匹配逻辑） =====
  const dmPolicy = dingtalkConfig.dmPolicy || 'open';
  const allowFrom = dingtalkConfig.allowFrom || [];
  const groupPolicy = dingtalkConfig.groupPolicy || 'open';
  const groupAllowFrom = dingtalkConfig.groupAllowFrom || [];

  // 私聊 allowlist 检查
  if (isDirect && dmPolicy === 'allowlist') {
    // 如果 allowFrom 为空列表，则不允许任何人
    if (allowFrom.length === 0) {
      log?.info?.(`[DingTalk] 私聊消息被拒绝: allowlist 为空，不允许任何人`);
      await sendMessage(dingtalkConfig, sessionWebhook, '⚠️ 机器人当前不接受私聊消息。', {
        atUserId: senderId,
      });
      return;
    }
    if (!isSenderInAllowlist(senderId, allowFrom)) {
      log?.info?.(`[DingTalk] 私聊消息被拒绝: sender=${senderId} 不在 allowlist 中`);
      await sendMessage(dingtalkConfig, sessionWebhook, '⚠️ 您没有权限与机器人私聊。', {
        atUserId: senderId,
      });
      return;
    }
    log?.info?.(`[DingTalk] 私聊 Allowlist 检查通过: sender=${senderId}`);
  }
  
  // 群聊 allowlist 检查
  if (!isDirect && groupPolicy === 'allowlist') {
    // 如果 groupAllowFrom 为空列表，则不允许任何人
    if (groupAllowFrom.length === 0) {
      log?.info?.(`[DingTalk] 群聊消息被拒绝: groupAllowlist 为空，不允许任何人`);
      await sendMessage(dingtalkConfig, sessionWebhook, '⚠️ 您没有权限在此群使用机器人。', {
        atUserId: senderId,
      });
      return;
    }
    if (!isSenderInAllowlist(senderId, groupAllowFrom)) {
      log?.info?.(`[DingTalk] 群聊消息被拒绝: sender=${senderId} 不在 groupAllowlist 中`);
      await sendMessage(dingtalkConfig, sessionWebhook, '⚠️ 您没有权限在此群使用机器人。', {
        atUserId: senderId,
      });
      return;
    }
    log?.info?.(`[DingTalk] 群聊 Allowlist 检查通过: sender=${senderId}`);
  }

  // 消息内容解析（图片/文件下载使用 api.dingtalk.com token，由 extractMessageContent 内部获取）
  const content = await extractMessageContent(data, dingtalkConfig, log);
  let oapiToken: string | null = await getOapiAccessToken(dingtalkConfig);
  if (!content.text) return;

  log?.info?.(`[DingTalk] 收到消息: from=${senderName} text="${content.text.slice(0, 50)}..."`);

  // ===== Session 管理 =====
  const sessionTimeout = dingtalkConfig.sessionTimeout ?? 1800000; // 默认 30 分钟
  const forceNewSession = isNewSessionCommand(content.text);

  // 如果是新会话命令，直接回复确认消息
  if (forceNewSession) {
    const { sessionKey } = getSessionKey(senderId, true, sessionTimeout, log);
    await sendMessage(dingtalkConfig, sessionWebhook, '✨ 已开启新会话，之前的对话已清空。', {
      atUserId: !isDirect ? senderId : null,
    });
    log?.info?.(`[DingTalk] 用户请求新会话: ${senderId}, newKey=${sessionKey}`);
    return;
  }

  // 获取或创建 session
  const { sessionKey, isNew } = getSessionKey(senderId, false, sessionTimeout, log);
  log?.info?.(`[DingTalk][Session] key=${sessionKey}, isNew=${isNew}`);

  // Gateway 认证：优先使用 token，其次 password
  const gatewayAuth = dingtalkConfig.gatewayToken || dingtalkConfig.gatewayPassword || '';

  // 构建 system prompts & 获取 oapi token（用于图片和文件后处理）
  const systemPrompts: string[] = [];
  if (dingtalkConfig.enableMediaUpload !== false) {
    // 添加图片和文件使用提示（告诉 LLM 直接输出本地路径或文件标记）
    systemPrompts.push(buildMediaSystemPrompt());
    // 如果前面获取失败，这里再尝试一次，避免因为临时错误完全失去媒体能力
    if (!oapiToken) {
      oapiToken = await getOapiAccessToken(dingtalkConfig);
    }
    log?.info?.(`[DingTalk][Media] oapiToken 获取${oapiToken ? '成功' : '失败'}`);
  } else {
    log?.info?.(`[DingTalk][Media] enableMediaUpload=false，跳过`);
  }

  // 自定义 system prompt
  if (dingtalkConfig.systemPrompt) {
    systemPrompts.push(dingtalkConfig.systemPrompt);
  }

  // useAIStreamCard=false 时跳过 AI Card，直接用普通消息
  const useAIStreamCard = dingtalkConfig.useAIStreamCard !== false;

  // 尝试创建 AI Card，并记录长任务起始时间
  const aiCardStartedAt = Date.now();
  const card = useAIStreamCard ? await createAICard(dingtalkConfig, data, log) : null;

  if (card) {
    // ===== AI Card 流式模式 =====
    log?.info?.(`[DingTalk] AI Card 创建成功: ${card.cardInstanceId}`);

    let accumulated = '';
    let lastUpdateTime = 0;
    const updateInterval = 300; // 最小更新间隔 ms
    let chunkCount = 0;

    try {
      log?.info?.(`[DingTalk] 开始请求 Gateway 流式接口...`);
      for await (const chunk of streamFromGateway({
        userContent: content.text,
        systemPrompts,
        sessionKey,
        gatewayAuth,
        log,
      })) {
        accumulated += chunk;
        chunkCount++;

        if (chunkCount <= 3) {
          log?.info?.(`[DingTalk] Gateway chunk #${chunkCount}: "${chunk.slice(0, 50)}..." (accumulated=${accumulated.length})`);
        }

        // 节流更新，避免过于频繁
        const now = Date.now();
        if (now - lastUpdateTime >= updateInterval) {
          // 实时清理文件、视频、音频标记（避免用户在流式过程中看到标记）
          const displayContent = accumulated
            .replace(FILE_MARKER_PATTERN, '')
            .replace(VIDEO_MARKER_PATTERN, '')
            .replace(AUDIO_MARKER_PATTERN, '')
            .trim();
          await streamAICard(card, displayContent, false, log);
          lastUpdateTime = now;
        }
      }

      log?.info?.(`[DingTalk] Gateway 流完成，共 ${chunkCount} chunks, ${accumulated.length} 字符`);

      // 后处理01：上传本地图片到钉钉，替换 file:// 路径为 media_id
      log?.info?.(`[DingTalk][Media] 开始图片后处理，内容片段="${accumulated.slice(0, 200)}..."`);
      accumulated = await processLocalImages(accumulated, oapiToken, log);

      // 【关键修复】AI Card 场景使用主动消息 API 发送文件/视频，避免 sessionWebhook 失效问题
      // 构建目标信息用于主动 API（isDirect 已在上面定义）
      const proactiveTarget: AICardTarget = isDirect
        ? { type: 'user', userId: data.senderStaffId || data.senderId }
        : { type: 'group', openConversationId: data.conversationId };

      // 后处理02：提取视频标记并发送视频消息（使用主动消息 API）
      log?.info?.(`[DingTalk][Video] 开始视频后处理 (使用主动API)`);
      accumulated = await processVideoMarkers(accumulated, '', dingtalkConfig, oapiToken, log, true, proactiveTarget, getAccessToken);

      // 后处理03：提取音频标记并发送音频消息（使用主动消息 API）
      log?.info?.(`[DingTalk][Audio] 开始音频后处理 (使用主动API)`);
      accumulated = await processAudioMarkers(accumulated, '', dingtalkConfig, oapiToken, log, true, proactiveTarget, getAccessToken);

      // 后处理04：提取文件标记并发送独立文件消息（使用主动消息 API）
      log?.info?.(`[DingTalk][File] 开始文件后处理 (使用主动API，目标=${JSON.stringify(proactiveTarget)})`);
      accumulated = await processFileMarkers(accumulated, sessionWebhook, dingtalkConfig, oapiToken, log, true, proactiveTarget, getAccessToken);

      // 完成 AI Card（如果内容为空，说明是纯媒体消息，使用默认提示）
      const finalContent = accumulated.trim();
      if (finalContent.length === 0) {
        log?.info?.(`[DingTalk][AICard] 内容为空（纯媒体消息），使用默认提示`);
        await finishAICard(card, '✅ 媒体已发送', log);
      } else {
        await finishAICard(card, finalContent, log);
      }
      log?.info?.(`[DingTalk] 流式响应完成，共 ${finalContent.length} 字符`);

      // 长任务完成后额外通知需求发起人（仅当耗时超过阈值）
      await notifyIfLongAICardTask({
        startedAt: aiCardStartedAt,
        isDirect,
        dingtalkConfig,
        sessionWebhook,
        senderId,
        log,
      });

    } catch (err: any) {
      log?.error?.(`[DingTalk] Gateway 调用失败: ${err.message}`);
      log?.error?.(`[DingTalk] 错误详情: ${err.stack}`);
      accumulated += `\n\n⚠️ 响应中断: ${err.message}`;
      try {
        await finishAICard(card, accumulated, log);
      } catch (finishErr: any) {
        log?.error?.(`[DingTalk] 错误恢复 finish 也失败: ${finishErr.message}`);
      }
    }

  } else {
    // ===== 普通消息模式（useAIStreamCard=false 或 AI Card 创建失败） =====
    if (!useAIStreamCard) {
      log?.info?.(`[DingTalk] useAIStreamCard=false，使用普通消息模式`);
      // 立即回复"任务已在后台运行"，让用户知道已收到请求
      await sendMessage(dingtalkConfig, sessionWebhook, '⏳ 任务已在后台运行，请稍候...', {
        atUserId: !isDirect ? senderId : null,
      });
    } else {
      log?.warn?.(`[DingTalk] AI Card 创建失败，降级为普通消息`);
    }

    let fullResponse = '';
    try {
      for await (const chunk of streamFromGateway({
        userContent: content.text,
        systemPrompts,
        sessionKey,
        gatewayAuth,
        log,
      })) {
        fullResponse += chunk;
      }

      // 后处理01：上传本地图片到钉钉，替换 file:// 路径为 media_id
      log?.info?.(`[DingTalk][Media] (降级模式) 开始图片后处理，内容片段="${fullResponse.slice(0, 200)}..."`);
      fullResponse = await processLocalImages(fullResponse, oapiToken, log);

      // 后处理02：提取视频标记并发送视频消息
      log?.info?.(`[DingTalk][Video] (降级模式) 开始视频后处理`);
      fullResponse = await processVideoMarkers(fullResponse, sessionWebhook, dingtalkConfig, oapiToken, log);

      // 后处理03：提取音频标记并发送音频消息
      log?.info?.(`[DingTalk][Audio] (降级模式) 开始音频后处理`);
      fullResponse = await processAudioMarkers(fullResponse, sessionWebhook, dingtalkConfig, oapiToken, log);

      // 后处理04：提取文件标记并发送独立文件消息
      log?.info?.(`[DingTalk][File] (降级模式) 开始文件后处理`);
      fullResponse = await processFileMarkers(fullResponse, sessionWebhook, dingtalkConfig, oapiToken, log);

      await sendMessage(dingtalkConfig, sessionWebhook, fullResponse || '（无响应）', {
        atUserId: !isDirect ? senderId : null,
        useMarkdown: true,
      });
      log?.info?.(`[DingTalk] 普通消息回复完成，共 ${fullResponse.length} 字符`);

    } catch (err: any) {
      log?.error?.(`[DingTalk] Gateway 调用失败: ${err.message}`);
      await sendMessage(dingtalkConfig, sessionWebhook, `抱歉，处理请求时出错: ${err.message}`, {
        atUserId: !isDirect ? senderId : null,
      });
    }
  }
}

// ============ 插件定义 ============

const meta = {
  id: 'dingtalk-connector',
  label: 'DingTalk',
  selectionLabel: 'DingTalk (钉钉)',
  docsPath: '/channels/dingtalk-connector',
  docsLabel: 'dingtalk-connector',
  blurb: '钉钉企业内部机器人，使用 Stream 模式，无需公网 IP，支持 AI Card 流式响应。',
  order: 70,
  aliases: ['dd', 'ding'],
};

const dingtalkPlugin = {
  id: 'dingtalk-connector',
  meta,
  capabilities: {
    chatTypes: ['direct', 'group'],
    reactions: false,
    threads: false,
    media: true,
    nativeCommands: false,
    blockStreaming: false,
  },
  reload: { configPrefixes: ['channels.dingtalk-connector'] },
  configSchema: {
    schema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        enabled: { type: 'boolean', default: true },
        clientId: { type: 'string', description: 'DingTalk App Key (Client ID)' },
        clientSecret: { type: 'string', description: 'DingTalk App Secret (Client Secret)' },
        enableMediaUpload: { type: 'boolean', default: true, description: 'Enable media upload prompt injection' },
        systemPrompt: { type: 'string', default: '', description: 'Custom system prompt' },
        dmPolicy: { type: 'string', enum: ['open', 'pairing', 'allowlist'], default: 'open' },
        allowFrom: { type: 'array', items: { type: 'string' }, description: 'Allowed sender IDs' },
        groupPolicy: { type: 'string', enum: ['open', 'allowlist'], default: 'open' },
        gatewayToken: { type: 'string', default: '', description: 'Gateway auth token (Bearer)' },
        gatewayPassword: { type: 'string', default: '', description: 'Gateway auth password (alternative to token)' },
        sessionTimeout: { type: 'number', default: 1800000, description: 'Session timeout in ms (default 30min)' },
        useAIStreamCard: { type: 'boolean', default: true, description: 'Use AI Card streaming; set false to reply via plain text' },
        debug: { type: 'boolean', default: false },
      },
      required: ['clientId', 'clientSecret'],
    },
    uiHints: {
      enabled: { label: 'Enable DingTalk' },
      clientId: { label: 'App Key', sensitive: false },
      clientSecret: { label: 'App Secret', sensitive: true },
      dmPolicy: { label: 'DM Policy' },
      groupPolicy: { label: 'Group Policy' },
    },
  },
  config: {
    listAccountIds: (cfg: ClawdbotConfig) => {
      const config = getConfig(cfg);
      return config.accounts
        ? Object.keys(config.accounts)
        : (isConfigured(cfg) ? ['default'] : []);
    },
    resolveAccount: (cfg: ClawdbotConfig, accountId?: string) => {
      const config = getConfig(cfg);
      const id = accountId || 'default';
      if (config.accounts?.[id]) {
        return { accountId: id, config: config.accounts[id], enabled: config.accounts[id].enabled !== false };
      }
      return { accountId: 'default', config, enabled: config.enabled !== false };
    },
    defaultAccountId: () => 'default',
    isConfigured: (account: any) => Boolean(account.config?.clientId && account.config?.clientSecret),
    describeAccount: (account: any) => ({
      accountId: account.accountId,
      name: account.config?.name || 'DingTalk',
      enabled: account.enabled,
      configured: Boolean(account.config?.clientId),
    }),
  },
  security: {
    resolveDmPolicy: ({ account }: any) => ({
      policy: account.config?.dmPolicy || 'open',
      allowFrom: account.config?.allowFrom || [],
      policyPath: 'channels.dingtalk-connector.dmPolicy',
      allowFromPath: 'channels.dingtalk-connector.allowFrom',
      approveHint: '使用 /allow dingtalk-connector:<userId> 批准用户',
      normalizeEntry: (raw: string) => raw.replace(/^(dingtalk-connector|dingtalk|dd|ding):/i, ''),
    }),
  },
  groups: {
    resolveRequireMention: ({ cfg }: any) => getConfig(cfg).groupPolicy !== 'open',
  },
  messaging: {
    // 注意：normalizeTarget 接收字符串，返回字符串（保持大小写，因为 openConversationId 是 base64 编码）
    normalizeTarget: (raw: string) => {
      if (!raw) return undefined;
      // 去掉渠道前缀，但保持原始大小写
      return raw.trim().replace(/^(dingtalk-connector|dingtalk|dd|ding):/i, '');
    },
    targetResolver: {
      // 支持普通 ID、Base64 编码的 conversationId，以及 user:/group: 前缀格式
      looksLikeId: (id: string) => /^(user:|group:)?[\w+/=-]+$/.test(id),
      hint: 'user:<userId> 或 group:<conversationId>',
    },
  },
  outbound: {
    deliveryMode: 'direct' as const,
    textChunkLimit: 4000,
    /**
     * 主动发送文本消息
     * @param ctx.to 目标格式：user:<userId> 或 group:<openConversationId>
     * @param ctx.text 消息内容
     * @param ctx.accountId 账号 ID
     */
    sendText: async (ctx: any) => {
      const { cfg, to, text, accountId, log } = ctx;
      const account = dingtalkPlugin.config.resolveAccount(cfg, accountId);
      const config = account?.config;

      if (!config?.clientId || !config?.clientSecret) {
        throw new Error('DingTalk not configured');
      }

      if (!to) {
        throw new Error('Target is required. Format: user:<userId> or group:<openConversationId>');
      }

      // 解析目标：user:<userId> 或 group:<openConversationId>
      const targetStr = String(to);
      let result: SendResult;

      log?.info?.(`[DingTalk][outbound.sendText] 解析目标: targetStr="${targetStr}"`);

      if (targetStr.startsWith('user:')) {
        const userId = targetStr.slice(5);
        log?.info?.(`[DingTalk][outbound.sendText] 发送给用户: userId="${userId}"`);
        result = await sendToUser(config, userId, text, { log });
      } else if (targetStr.startsWith('group:')) {
        const openConversationId = targetStr.slice(6);
        log?.info?.(`[DingTalk][outbound.sendText] 发送到群: openConversationId="${openConversationId}"`);
        result = await sendToGroup(config, openConversationId, text, { log });
      } else {
        // 默认当作 userId 处理
        log?.info?.(`[DingTalk][outbound.sendText] 默认发送给用户: userId="${targetStr}"`);
        result = await sendToUser(config, targetStr, text, { log });
      }

      if (result.ok) {
        return { channel: 'dingtalk-connector', messageId: result.processQueryKey || 'unknown' };
      }
      throw new Error(result.error || 'Failed to send message');
    },
    /**
     * 主动发送媒体消息（图片）
     * @param ctx.to 目标格式：user:<userId> 或 group:<openConversationId>
     * @param ctx.text 消息文本/标题
     * @param ctx.mediaUrl 媒体 URL（钉钉仅支持图片 URL）
     * @param ctx.accountId 账号 ID
     */
    sendMedia: async (ctx: any) => {
      const { cfg, to, text, mediaUrl, accountId, log } = ctx;
      const account = dingtalkPlugin.config.resolveAccount(cfg, accountId);
      const config = account?.config;

      if (!config?.clientId || !config?.clientSecret) {
        throw new Error('DingTalk not configured');
      }

      if (!to) {
        throw new Error('Target is required. Format: user:<userId> or group:<openConversationId>');
      }

      // 解析目标
      const targetStr = String(to);
      let result: SendResult;

      // 如果有媒体 URL，发送图片消息
      if (mediaUrl) {
        if (targetStr.startsWith('user:')) {
          const userId = targetStr.slice(5);
          result = await sendToUser(config, userId, mediaUrl, { msgType: 'image', log });
        } else if (targetStr.startsWith('group:')) {
          const openConversationId = targetStr.slice(6);
          result = await sendToGroup(config, openConversationId, mediaUrl, { msgType: 'image', log });
        } else {
          result = await sendToUser(config, targetStr, mediaUrl, { msgType: 'image', log });
        }
      } else {
        // 无媒体，发送文本
        if (targetStr.startsWith('user:')) {
          const userId = targetStr.slice(5);
          result = await sendToUser(config, userId, text || '', { log });
        } else if (targetStr.startsWith('group:')) {
          const openConversationId = targetStr.slice(6);
          result = await sendToGroup(config, openConversationId, text || '', { log });
        } else {
          result = await sendToUser(config, targetStr, text || '', { log });
        }
      }

      if (result.ok) {
        return { channel: 'dingtalk-connector', messageId: result.processQueryKey || 'unknown' };
      }
      throw new Error(result.error || 'Failed to send media');
    },
  },
  gateway: {
    startAccount: async (ctx: any) => {
      const { account, cfg, abortSignal } = ctx;
      const config = account.config;

      if (!config.clientId || !config.clientSecret) {
        throw new Error('DingTalk clientId and clientSecret are required');
      }

      ctx.log?.info(`[${account.accountId}] 启动钉钉 Stream 客户端...`);

      const client = new DWClient({
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        debug: config.debug || false,
      });

      client.registerCallbackListener(TOPIC_ROBOT, async (res: any) => {
        const messageId = res.headers?.messageId;
        ctx.log?.info?.(`[DingTalk] 收到 Stream 回调, messageId=${messageId}, headers=${JSON.stringify(res.headers)}`);

        // 【关键修复】立即确认回调，避免钉钉服务器因超时而重发
        // 钉钉 Stream 模式要求及时响应，否则约60秒后会重发消息
        if (messageId) {
          client.socketCallBackResponse(messageId, { success: true });
          ctx.log?.info?.(`[DingTalk] 已立即确认回调: messageId=${messageId}`);
        }

        // 【消息去重】检查是否已处理过该消息
        if (messageId && isMessageProcessed(messageId)) {
          ctx.log?.warn?.(`[DingTalk] 检测到重复消息，跳过处理: messageId=${messageId}`);
          return;
        }

        // 标记消息为已处理
        if (messageId) {
          markMessageProcessed(messageId);
        }

        // 异步处理消息（不阻塞回调确认）
        try {
          ctx.log?.info?.(`[DingTalk] 原始 data: ${typeof res.data === 'string' ? res.data.slice(0, 500) : JSON.stringify(res.data).slice(0, 500)}`);
          const data = JSON.parse(res.data);

          await handleDingTalkMessage({
            cfg,
            accountId: account.accountId,
            data,
            sessionWebhook: data.sessionWebhook,
            log: ctx.log,
            dingtalkConfig: config,
          });
        } catch (error: any) {
          ctx.log?.error?.(`[DingTalk] 处理消息异常: ${error.message}`);
          // 注意：即使处理失败，也不需要再次响应（已经提前确认了）
        }
      });

      await client.connect();
      ctx.log?.info(`[${account.accountId}] 钉钉 Stream 客户端已连接`);

      const rt = getRuntime();
      rt.channel.activity.record('dingtalk-connector', account.accountId, 'start');

      let stopped = false;
      if (abortSignal) {
        abortSignal.addEventListener('abort', () => {
          if (stopped) return;
          stopped = true;
          ctx.log?.info(`[${account.accountId}] 停止钉钉 Stream 客户端...`);
          rt.channel.activity.record('dingtalk-connector', account.accountId, 'stop');
        });
      }

      return {
        stop: () => {
          if (stopped) return;
          stopped = true;
          ctx.log?.info(`[${account.accountId}] 钉钉 Channel 已停止`);
          rt.channel.activity.record('dingtalk-connector', account.accountId, 'stop');
        },
      };
    },
  },
  status: {
    defaultRuntime: { accountId: 'default', running: false, lastStartAt: null, lastStopAt: null, lastError: null },
    probe: async ({ cfg }: any) => {
      if (!isConfigured(cfg)) return { ok: false, error: 'Not configured' };
      try {
        const config = getConfig(cfg);
        await getAccessToken(config);
        return { ok: true, details: { clientId: config.clientId } };
      } catch (error: any) {
        return { ok: false, error: error.message };
      }
    },
    buildChannelSummary: ({ snapshot }: any) => ({
      configured: snapshot?.configured ?? false,
      running: snapshot?.running ?? false,
      lastStartAt: snapshot?.lastStartAt ?? null,
      lastStopAt: snapshot?.lastStopAt ?? null,
      lastError: snapshot?.lastError ?? null,
    }),
  },
};

// ============ 插件注册 ============

const plugin = {
  id: 'dingtalk-connector',
  name: 'DingTalk Channel',
  description: 'DingTalk (钉钉) messaging channel via Stream mode with AI Card streaming',
  configSchema: {
    type: 'object',
    additionalProperties: true,
    properties: { enabled: { type: 'boolean', default: true } },
  },
  register(api: ClawdbotPluginApi) {
    runtime = api.runtime;
    api.registerChannel({ plugin: dingtalkPlugin });

    // ===== Gateway Methods =====

    api.registerGatewayMethod('dingtalk-connector.status', async ({ respond, cfg }: any) => {
      const result = await dingtalkPlugin.status.probe({ cfg });
      respond(true, result);
    });

    api.registerGatewayMethod('dingtalk-connector.probe', async ({ respond, cfg }: any) => {
      const result = await dingtalkPlugin.status.probe({ cfg });
      respond(result.ok, result);
    });

    /**
     * 主动发送单聊消息
     * 参数：
     *   - userId / userIds: 目标用户 ID（支持单个或数组）
     *   - content: 消息内容
     *   - msgType?: 'text' | 'markdown' | 'link' | 'actionCard' | 'image'（降级时使用，默认 text）
     *   - title?: markdown 消息标题
     *   - useAICard?: 是否使用 AI Card（默认 true）
     *   - fallbackToNormal?: AI Card 失败时是否降级到普通消息（默认 true）
     *   - accountId?: 使用的账号 ID（默认 default）
     */
    api.registerGatewayMethod('dingtalk-connector.sendToUser', async ({ respond, cfg, params, log }: any) => {
      const { userId, userIds, content, msgType, title, useAICard, fallbackToNormal, accountId } = params || {};
      const account = dingtalkPlugin.config.resolveAccount(cfg, accountId);

      if (!account.config?.clientId) {
        return respond(false, { error: 'DingTalk not configured' });
      }

      const targetUserIds = userIds || (userId ? [userId] : []);
      if (targetUserIds.length === 0) {
        return respond(false, { error: 'userId or userIds is required' });
      }

      if (!content) {
        return respond(false, { error: 'content is required' });
      }

      const result = await sendToUser(account.config, targetUserIds, content, {
        msgType,
        title,
        log,
        useAICard: useAICard !== false,  // 默认 true
        fallbackToNormal: fallbackToNormal !== false,  // 默认 true
      });
      respond(result.ok, result);
    });

    /**
     * 主动发送群聊消息
     * 参数：
     *   - openConversationId: 群会话 ID
     *   - content: 消息内容
     *   - msgType?: 'text' | 'markdown' | 'link' | 'actionCard' | 'image'（降级时使用，默认 text）
     *   - title?: markdown 消息标题
     *   - useAICard?: 是否使用 AI Card（默认 true）
     *   - fallbackToNormal?: AI Card 失败时是否降级到普通消息（默认 true）
     *   - accountId?: 使用的账号 ID（默认 default）
     */
    api.registerGatewayMethod('dingtalk-connector.sendToGroup', async ({ respond, cfg, params, log }: any) => {
      const { openConversationId, content, msgType, title, useAICard, fallbackToNormal, accountId } = params || {};
      const account = dingtalkPlugin.config.resolveAccount(cfg, accountId);

      if (!account.config?.clientId) {
        return respond(false, { error: 'DingTalk not configured' });
      }

      if (!openConversationId) {
        return respond(false, { error: 'openConversationId is required' });
      }

      if (!content) {
        return respond(false, { error: 'content is required' });
      }

      const result = await sendToGroup(account.config, openConversationId, content, {
        msgType,
        title,
        log,
        useAICard: useAICard !== false,  // 默认 true
        fallbackToNormal: fallbackToNormal !== false,
      });
      respond(result.ok, result);
    });

    /**
     * 智能发送消息（自动检测目标类型和消息格式）
     * 参数：
     *   - target: 目标（user:<userId> 或 group:<openConversationId>）
     *   - content: 消息内容
     *   - msgType?: 消息类型（降级时使用，可选，不指定则自动检测）
     *   - title?: 标题（用于 markdown）
     *   - useAICard?: 是否使用 AI Card（默认 true）
     *   - fallbackToNormal?: AI Card 失败时是否降级到普通消息（默认 true）
     *   - accountId?: 账号 ID
     */
    api.registerGatewayMethod('dingtalk-connector.send', async ({ respond, cfg, params, log }: any) => {
      const { target, content, message, msgType, title, useAICard, fallbackToNormal, accountId } = params || {};
      const actualContent = content || message;  // 兼容 message 字段
      const account = dingtalkPlugin.config.resolveAccount(cfg, accountId);

      log?.info?.(`[DingTalk][Send] 收到请求: params=${JSON.stringify(params)}`);

      if (!account.config?.clientId) {
        return respond(false, { error: 'DingTalk not configured' });
      }

      if (!target) {
        return respond(false, { error: 'target is required (format: user:<userId> or group:<openConversationId>)' });
      }

      if (!actualContent) {
        return respond(false, { error: 'content is required' });
      }

      const targetStr = String(target);
      let sendTarget: { userId?: string; openConversationId?: string };

      if (targetStr.startsWith('user:')) {
        sendTarget = { userId: targetStr.slice(5) };
      } else if (targetStr.startsWith('group:')) {
        sendTarget = { openConversationId: targetStr.slice(6) };
      } else {
        // 默认当作 userId
        sendTarget = { userId: targetStr };
      }

      log?.info?.(`[DingTalk][Send] 解析后目标: sendTarget=${JSON.stringify(sendTarget)}`);

      const result = await sendProactive(account.config, sendTarget, actualContent, {
        msgType,
        title,
        log,
        useAICard: useAICard !== false,  // 默认 true
        fallbackToNormal: fallbackToNormal !== false,
      });
      respond(result.ok, result);
    });

    api.logger?.info('[DingTalk]1.2 插件已注册（支持主动发送 AI Card 消息）');
  },
};

export default plugin;
export {
  dingtalkPlugin,
  // 回复消息（需要 sessionWebhook）
  sendMessage,
  sendTextMessage,
  sendMarkdownMessage,
  // 主动发送消息（无需 sessionWebhook）
  sendToUser,
  sendToGroup,
  sendProactive,
};
