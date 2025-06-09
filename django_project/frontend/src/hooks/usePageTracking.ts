import { useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { pageView } from '@/utils/analytics';


const usePageTracking = () => {
    const location = useLocation();

    useEffect(() => {
        // Track page view on initial load and when location changes
        const pagePath = location.pathname + location.search;
        const pageTitle = document.title || 'Default Page Title';
        pageView(pagePath, pageTitle);
    }, [location]);

}

export default usePageTracking;