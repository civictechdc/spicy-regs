"use strict";

import { MessagesProps, RenderMessageProps } from "@copilotkit/react-ui";
import { ThinkingProcess } from "./ThinkingProcess";
// import { Message } from "@copilotkit/runtime-client-gql"; 
import { Message } from "@copilotkit/shared";
import React, { useMemo } from "react";

// We need to assume the underlying message objects have these methods as seen in source
// or use type guards if available.
// For now, we will cast to any to access isActionExecutionMessage etc if TS complains,
// but better to import types correctly if possible.
// The props say message is Message from @copilotkit/shared.

export function CustomMessages(props: MessagesProps) {
  const { messages, RenderMessage, inProgress, ...rest } = props;

  const groupedMessages = useMemo(() => {
    const groups: (Message | Message[])[] = [];
    let currentThinkingGroup: Message[] = [];

    messages.forEach((msg) => {
      const m = msg as any;
      const isThinking =
        m.isActionExecutionMessage?.() ||
        m.isResultMessage?.() ||
        m.isAgentStateMessage?.() ||
        (m.role === "function" || m.role === "tool"); // Fallback

      if (isThinking) {
        currentThinkingGroup.push(msg);
      } else {
        // Text message (User or Assistant) or System
        if (currentThinkingGroup.length > 0) {
          groups.push(currentThinkingGroup);
          currentThinkingGroup = [];
        }
        groups.push(msg);
      }
    });

    if (currentThinkingGroup.length > 0) {
      groups.push(currentThinkingGroup);
    }
    
    return groups;
  }, [messages]);

  let msgIndex = 0;

  return (
    <div className="flex flex-col gap-4 p-4 flex-1 overflow-y-auto w-full">
      {groupedMessages.map((group, groupIndex) => {
        if (Array.isArray(group)) {
          // Thinking Group
          const logs = group.map(m => {
            const anyM = m as any;
            if (anyM.isActionExecutionMessage?.()) {
              return `Action: ${anyM.name}(${JSON.stringify(anyM.arguments)})`;
            }
            if (anyM.isResultMessage?.()) {
              return `Result: ${anyM.result}`;
            }
            if (anyM.isAgentStateMessage?.()) {
              return `State: ${JSON.stringify(anyM.state)}`;
            }
            return `Log: ${JSON.stringify(m)}`;
          });
          
          return <ThinkingProcess key={`thinking-${groupIndex}`} logs={logs} isOpen={true} />;
        } else {
          // Standard Message
          const m = group;
          const isCurrentMessage = msgIndex === messages.length - 1;
          // We need to call RenderMessage. 
          // Note: RenderMessage implementation usually handles finding the component to use (AssistantMessage, UserMessage etc)
          // based on the message type.
          
          const el = (
            <RenderMessage
              key={m.id || `msg-${groupIndex}`}
              message={m}
              inProgress={inProgress}
              index={msgIndex}
              isCurrentMessage={isCurrentMessage}
              {...rest}
            />
          );
          msgIndex++;
          return el;
        }
      })}
    </div>
  );
}
