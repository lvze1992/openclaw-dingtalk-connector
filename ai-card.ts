/**
 * 钉钉 AI Card 流式卡片：创建、流式更新、完成
 * 模板 45c9632d，流式字段为 content，cardParamMap 含 config / content / flowStatus / sys_full_json_obj
 */

import axios from 'axios';
import { getAccessToken } from './tokens';

const DINGTALK_API = 'https://api.dingtalk.com';
const AI_CARD_TEMPLATE_ID = '45c9632d-ba6e-4cf6-b1c4-39238b264977.schema';

const AICardStatus = {
  PROCESSING: '1',
  INPUTING: '2',
  FINISHED: '3',
  EXECUTING: '4',
  FAILED: '5',
} as const;

export type AICardTarget =
  | { type: 'user'; userId: string }
  | { type: 'group'; openConversationId: string };

export interface AICardInstance {
  cardInstanceId: string;
  accessToken: string;
  inputingStarted: boolean;
}

function buildCardParamMap(content: string, flowStatus: string): Record<string, string> {
  return {
    config: JSON.stringify({ autoLayout: true }),
    content,
    flowStatus,
    sys_full_json_obj: JSON.stringify({ order: ['config', 'content', 'flowStatus'] }),
  };
}

function buildDeliverBody(cardInstanceId: string, target: AICardTarget, robotCode: string): any {
  const base = { outTrackId: cardInstanceId, userIdType: 1 };
  if (target.type === 'group') {
    return { ...base, openSpaceId: `dtv1.card//IM_GROUP.${target.openConversationId}`, imGroupOpenDeliverModel: { robotCode } };
  }
  return { ...base, openSpaceId: `dtv1.card//IM_ROBOT.${target.userId}`, imRobotOpenDeliverModel: { spaceType: 'IM_ROBOT' } };
}

/**
 * 创建 AI Card 实例（被动回复场景：从回调 data 提取目标）
 */
export async function createAICard(
  config: any,
  data: any,
  log?: any,
): Promise<AICardInstance | null> {
  const isGroup = data.conversationType === '2';
  log?.info?.(`[DingTalk][AICard] conversationType=${data.conversationType}, conversationId=${data.conversationId}, senderStaffId=${data.senderStaffId}, senderId=${data.senderId}`);
  const target: AICardTarget = isGroup
    ? { type: 'group', openConversationId: data.conversationId }
    : { type: 'user', userId: data.senderStaffId || data.senderId };
  return createAICardForTarget(config, target, log);
}

/**
 * 流式更新 AI Card 内容（key=content）
 */
export async function streamAICard(
  card: AICardInstance,
  content: string,
  finished: boolean = false,
  log?: any,
): Promise<void> {
  if (!card.inputingStarted) {
    const statusBody = {
      outTrackId: card.cardInstanceId,
      cardData: { cardParamMap: buildCardParamMap('', AICardStatus.INPUTING) },
    };
    log?.info?.(`[DingTalk][AICard] PUT /v1.0/card/instances (初始化) outTrackId=${card.cardInstanceId}`);
    try {
      await axios.put(`${DINGTALK_API}/v1.0/card/instances`, statusBody, {
        headers: { 'x-acs-dingtalk-access-token': card.accessToken, 'Content-Type': 'application/json' },
      });
    } catch (err: any) {
      log?.error?.(`[DingTalk][AICard] 初始化失败: ${err.message}, resp=${JSON.stringify(err.response?.data)}`);
      throw err;
    }
    card.inputingStarted = true;
  }

  const body = {
    outTrackId: card.cardInstanceId,
    guid: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    key: 'content',
    content,
    isFull: true,
    isFinalize: finished,
    isError: false,
  };
  log?.info?.(`[DingTalk][AICard] PUT /v1.0/card/streaming contentLen=${content.length} isFinalize=${finished} guid=${body.guid}`);
  try {
    await axios.put(`${DINGTALK_API}/v1.0/card/streaming`, body, {
      headers: { 'x-acs-dingtalk-access-token': card.accessToken, 'Content-Type': 'application/json' },
    });
  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard] streaming 更新失败: ${err.message}, resp=${JSON.stringify(err.response?.data)}`);
    throw err;
  }
}

/**
 * 完成 AI Card：先 streaming isFinalize，再 PUT 最终 cardParamMap
 */
export async function finishAICard(card: AICardInstance, content: string, log?: any): Promise<void> {
  log?.info?.(`[DingTalk][AICard] 开始 finish，最终内容长度=${content.length}`);
  await streamAICard(card, content, true, log);
  const body = {
    outTrackId: card.cardInstanceId,
    cardData: { cardParamMap: buildCardParamMap(content, AICardStatus.FINISHED) },
  };
  log?.info?.(`[DingTalk][AICard] PUT /v1.0/card/instances (完成) outTrackId=${card.cardInstanceId}`);
  try {
    await axios.put(`${DINGTALK_API}/v1.0/card/instances`, body, {
      headers: { 'x-acs-dingtalk-access-token': card.accessToken, 'Content-Type': 'application/json' },
    });
  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard] 完成更新失败: ${err.message}, resp=${JSON.stringify(err.response?.data)}`);
  }
}

/**
 * 通用：为指定目标创建 AI Card 实例（单聊/群聊）
 */
export async function createAICardForTarget(
  config: any,
  target: AICardTarget,
  log?: any,
): Promise<AICardInstance | null> {
  const targetDesc = target.type === 'group' ? `群聊 ${target.openConversationId}` : `用户 ${target.userId}`;
  try {
    const token = await getAccessToken(config);
    const cardInstanceId = `card_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    log?.info?.(`[DingTalk][AICard] 开始创建卡片: ${targetDesc}, outTrackId=${cardInstanceId}`);

    const createBody = {
      cardTemplateId: AI_CARD_TEMPLATE_ID,
      outTrackId: cardInstanceId,
      cardData: { cardParamMap: buildCardParamMap('', AICardStatus.INPUTING) },
      callbackType: 'STREAM',
      imGroupOpenSpaceModel: { supportForward: true },
      imRobotOpenSpaceModel: { supportForward: true },
    };
    await axios.post(`${DINGTALK_API}/v1.0/card/instances`, createBody, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
    });

    const deliverBody = buildDeliverBody(cardInstanceId, target, config.clientId);
    log?.info?.(`[DingTalk][AICard] POST /v1.0/card/instances/deliver body=${JSON.stringify(deliverBody)}`);
    await axios.post(`${DINGTALK_API}/v1.0/card/instances/deliver`, deliverBody, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
    });

    return { cardInstanceId, accessToken: token, inputingStarted: false };
  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard] 创建卡片失败 (${targetDesc}): ${err.message}`);
    if (err.response) {
      log?.error?.(`[DingTalk][AICard] 错误响应: status=${err.response.status} data=${JSON.stringify(err.response.data)}`);
    }
    return null;
  }
}
