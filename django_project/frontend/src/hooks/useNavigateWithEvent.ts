import { useNavigate } from 'react-router-dom';
import { clickEvent } from '@/utils/analytics';

export const useNavigateWithEvent = () => {
    const navigate = useNavigate();

    const navigateWithEvent = (path: string, eventName?: string, skipEvent: boolean = false) => {
        if (!skipEvent) {
            clickEvent(eventName || path.replaceAll('/', '_'));
        }        

        // Navigate to the specified path
        navigate(path);
    };

    return navigateWithEvent;
}
