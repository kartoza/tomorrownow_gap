import React, { useEffect, useState} from "react";
import { Flex } from "@chakra-ui/react";
import { useLocation } from 'react-router-dom';
import HeroSection from "@/features/landing/HeroSection";
import BridgingGapSection from "@/features/landing/BridgingGapSection";
import AgroMetHubSection from "@/features/landing/AgroMetHubSection";
import GLobalImpactSection from "@/features/landing/GlobalImpactSection";
import PartnersSection from "@/features/landing/PartnersSection";
import AboutUsSection from "@/features/landing/AboutUsSection";
import { useScrollSpy } from "@/hooks/useScrollSpy";
import { handleSmoothScroll } from "@/utils/scroll";

const sectionIds = ['hero', 'products', 'bridging', 'impact', 'partners', 'about'];

const LandingPage: React.FC = () => {
    // Scroll spy for active section highlighting
    useScrollSpy(sectionIds);

    const location = useLocation();

    // Handle hash navigation when component mounts
    useEffect(() => {
        if (location.hash) {
            // Small delay to ensure page is rendered
            setTimeout(() => {
                handleSmoothScroll(location.hash);
            }, 100);
        }
    }, [location.hash]);

    return (
        <Flex direction="column" w="full">
            <HeroSection />
            <AgroMetHubSection />
            <BridgingGapSection />
            <GLobalImpactSection />
            <PartnersSection />
            <AboutUsSection />
        </Flex>
    );
};

export default LandingPage;