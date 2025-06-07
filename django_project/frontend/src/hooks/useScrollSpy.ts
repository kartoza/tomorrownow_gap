import { useEffect } from 'react';
import { useScrollContext } from '@/context/ScrollContext';

export const useScrollSpy = (sectionIds: string[], offset: number = 100) => {
  const { setActiveSection } = useScrollContext();

  useEffect(() => {
    const handleScroll = () => {
        const scrollPosition = window.scrollY + offset; // Offset for better UX
        for (const section of sectionIds) {
          const element = document.getElementById(section);
          if (element) {
              let { offsetTop, offsetHeight } = element;
              if (section === 'partners') {
                offsetHeight = offsetHeight - 100;
              } else if (section === 'about') {
                offsetTop = offsetTop - 70;
              }
              if (scrollPosition >= offsetTop && scrollPosition < offsetTop + offsetHeight) {
                setActiveSection(`#${section}`);
                break;
              }
          }
        }

        // Clear active section if we're at the top
        if (window.scrollY < offset) {
            setActiveSection('');
        }
    };

    window.addEventListener('scroll', handleScroll);
    handleScroll(); // Check initial position

    return () => window.removeEventListener('scroll', handleScroll);
  }, [sectionIds, offset, setActiveSection]);
};