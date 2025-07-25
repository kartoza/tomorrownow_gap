import { useNavigate, NavigateOptions } from 'react-router-dom';
import { clickEvent } from '@/utils/analytics';

const USE_HARD_REDIRECT_PATHS = [
    '/api/v1/docs/',
]


export const useNavigateWithEvent = () => {
    const navigate = useNavigate();

    const navigateWithEvent = (path: string, eventName?: string, skipEvent: boolean = false, options?: NavigateOptions) => {
        if (!skipEvent) {
            clickEvent(eventName || path.replaceAll('/', '_'));
        }

        if (USE_HARD_REDIRECT_PATHS.includes(path)) {
            // Use hard redirect for specific paths
            window.location.href = path;
            return;
        }

        // Navigate to the specified path
        navigate(path, options);
    };

    return navigateWithEvent;
}
