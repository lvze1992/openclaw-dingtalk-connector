/**
 * 钉钉文本/Markdown 消息发送工具（通过 sessionWebhook 被动回复）。
 */

import axios from 'axios';
import { getAccessToken } from './tokens';

export async function sendMarkdownMessage(
  config: any,
  sessionWebhook: string,
  title: string,
  markdown: string,
  options: any = {},
): Promise<any> {
  const token = await getAccessToken(config);
  let text = markdown;
  if (options.atUserId) text = `${text} @${options.atUserId}`;

  const body: any = {
    msgtype: 'markdown',
    markdown: { title: title || 'Moltbot', text },
  };
  if (options.atUserId) body.at = { atUserIds: [options.atUserId], isAtAll: false };

  return (
    await axios.post(sessionWebhook, body, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
    })
  ).data;
}

export async function sendTextMessage(
  config: any,
  sessionWebhook: string,
  text: string,
  options: any = {},
): Promise<any> {
  const token = await getAccessToken(config);
  const body: any = { msgtype: 'text', text: { content: text } };
  if (options.atUserId) body.at = { atUserIds: [options.atUserId], isAtAll: false };

  return (
    await axios.post(sessionWebhook, body, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
    })
  ).data;
}

/** 智能选择 text / markdown，根据内容自动判断 */
export async function sendMessage(
  config: any,
  sessionWebhook: string,
  text: string,
  options: any = {},
): Promise<any> {
  const hasMarkdown = /^[#*>-]|[*_`#\[\]]/.test(text) || text.includes('\n');
  const useMarkdown = options.useMarkdown !== false && (options.useMarkdown || hasMarkdown);

  if (useMarkdown) {
    const title =
      options.title ||
      text.split('\n')[0].replace(/^[#*\s\->]+/, '').slice(0, 20) ||
      'Moltbot';
    return sendMarkdownMessage(config, sessionWebhook, title, text, options);
  }
  return sendTextMessage(config, sessionWebhook, text, options);
}
