import { useNavigate, NavigateOptions } from 'react-router-dom';
import { clickEvent } from '@/utils/analytics';

export const useNavigateWithEvent = () => {
    const navigate = useNavigate();

    const navigateWithEvent = (path: string, eventName?: string, skipEvent: boolean = false, options?: NavigateOptions) => {
        if (!skipEvent) {
            clickEvent(eventName || path.replaceAll('/', '_'));
        }        

        // Navigate to the specified path
        navigate(path, options);
    };

    return navigateWithEvent;
}
