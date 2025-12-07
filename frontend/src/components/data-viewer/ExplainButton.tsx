import { useCopilotChat } from "@copilotkit/react-core";
import { TextMessage, MessageRole } from "@copilotkit/runtime-client-gql";

interface ExplainButtonProps {
  itemTitle: string;
  dataType: string;
}

export function ExplainButton({ 
  itemTitle, 
  dataType 
}: ExplainButtonProps) {
  const { appendMessage } = useCopilotChat();
  
  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        appendMessage(
          new TextMessage({
            role: MessageRole.User,
            content: `Please explain this ${dataType} titled '${itemTitle}'. Focus on its significance and key details.`
          })
        );
        
        // Hack to open the sidebar if it's closed, since CopilotSidebar is uncontrolled
        const windowEl = document.querySelector('.copilotKitWindow');
        const buttonEl = document.querySelector('.copilotKitButton');
        const isOpen = windowEl?.classList.contains('open');
        
        if (!isOpen && buttonEl instanceof HTMLElement) {
          buttonEl.click();
        }
      }}
      className="p-2 ml-1 text-blue-500 hover:text-blue-700 dark:text-blue-400 dark:hover:text-blue-200 hover:bg-blue-50 dark:hover:bg-blue-900/30 rounded-full transition-colors"
      title="Explain this with AI"
    >
      <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M7.9 20A9 9 0 1 0 4 16.1L2 22Z"/>
        <path d="M8 12h.01"/>
        <path d="M12 12h.01"/>
        <path d="M16 12h.01"/>
      </svg>
    </button>
  );
}
