import { useEffect, useRef } from 'react';
import { useScrollContext } from '@/context/ScrollContext';
import { trackSectionView, trackSectionEngagement } from '@/utils/analytics';

interface SectionTimer {
  startTime: number;
  timeoutId: NodeJS.Timeout | null;
  isQualified: boolean; // Track if this section met the minimum threshold
}

export const useScrollSpy = (
  sectionIds: string[],
  offset: number = 100,
  minViewDuration: number = 3000, // 3 seconds in milliseconds
  trackReturnVisits: boolean = false, // Set to true if you want to track every visit to a section
  pageTitle: string = 'landing_page'
) => {
  const { setActiveSection } = useScrollContext();
  const viewedSections = useRef<Set<string>>(new Set());
  const lastActiveSection = useRef<string>('');
  const sectionTimers = useRef<Map<string, SectionTimer>>(new Map());
  const setActiveSectionRef = useRef(setActiveSection);

  // Update ref when setActiveSection changes, but don't trigger effect re-run
  useEffect(() => {
    setActiveSectionRef.current = setActiveSection;
  }, [setActiveSection]);

  const startSectionTimer = (sectionId: string) => {
    // Clear any existing timer for this section
    const existingTimer = sectionTimers.current.get(sectionId);
    if (existingTimer?.timeoutId) {
      clearTimeout(existingTimer.timeoutId);
    }

    const startTime = Date.now();
    
    // Set timeout to mark section as "qualified" after minimum duration
    const timeoutId = setTimeout(() => {
      // Mark this section as qualified for engagement tracking
      sectionTimers.current.set(sectionId, {
        startTime,
        timeoutId: null,
        isQualified: true
      });
    }, minViewDuration);

    sectionTimers.current.set(sectionId, {
      startTime,
      timeoutId,
      isQualified: false
    });
  };

  const stopSectionTimer = (sectionId: string) => {
    const timer = sectionTimers.current.get(sectionId);
    if (timer) {
      const actualDuration = Date.now() - timer.startTime;

      if (timer.timeoutId) {
        // User left before minimum duration - clear the timer, no engagement event
        clearTimeout(timer.timeoutId);
      } else if (timer.isQualified) {
        // Section was qualified AND user is now leaving - send engagement event with ACTUAL duration
        trackSectionEngagement(sectionId, pageTitle, actualDuration);
      }
      
      sectionTimers.current.delete(sectionId);
    }
  };

  const clearAllTimers = () => {
    sectionTimers.current.forEach((timer) => {
      if (timer.timeoutId) {
        clearTimeout(timer.timeoutId);
      }
    });
    sectionTimers.current.clear();
  };

  useEffect(() => {
    const handleScroll = () => {
        const scrollPosition = window.scrollY + offset; // Offset for better UX
        let currentActiveSection = '';
        
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
                currentActiveSection = section;
                setActiveSectionRef.current(`#${section}`);

                // Track section view based on settings
                const isFirstView = !viewedSections.current.has(section);
                const shouldTrackView = isFirstView || (trackReturnVisits && lastActiveSection.current !== section);
                
                if (shouldTrackView) {
                  viewedSections.current.add(section);
                  trackSectionView(section, pageTitle, isFirstView);
                }

                // Start timer for duration tracking if this is a new section
                if (lastActiveSection.current !== section) {
                  // Stop timer for previous section
                  if (lastActiveSection.current) {
                    stopSectionTimer(lastActiveSection.current);
                  }
                  
                  // Start timer for current section
                  startSectionTimer(section);
                  lastActiveSection.current = section;
                }

                break;
              }
          }
        }

        // Clear active section if we're at the top
        if (window.scrollY < offset) {
          // Stop any active timers
          if (lastActiveSection.current) {
            stopSectionTimer(lastActiveSection.current);
          }
          setActiveSectionRef.current('');
          lastActiveSection.current = '';
        }
    };

    window.addEventListener('scroll', handleScroll);
    handleScroll(); // Check initial position

    return () => {
      window.removeEventListener('scroll', handleScroll);
      clearAllTimers();
    };
  }, [sectionIds, offset, minViewDuration, trackReturnVisits]);

  // Cleanup timers when component unmounts
  useEffect(() => {
    return () => {
      clearAllTimers();
    };
  }, []);

};