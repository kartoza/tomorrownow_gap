import React, { useEffect, useState} from "react";
import { Flex } from "@chakra-ui/react";
import HeroSection from "@/features/landing/HeroSection";
import BridgingGapSection from "@/features/landing/BridgingGapSection";
import AgroMetHubSection from "@/features/landing/AgroMetHubSection";
import GLobalImpactSection from "@/features/landing/GlobalImpactSection";
import PartnersSection from "@/features/landing/PartnersSection";
import AboutUsSection from "@/features/landing/AboutUsSection";
import { useScrollSpy } from "@/hooks/useScrollSpy";


const LandingPage: React.FC = () => {
    // Scroll spy for active section highlighting
    const sectionIds = ['hub', 'partners', 'about'];
    useScrollSpy(sectionIds);

    return (
        <Flex direction="column" w="full">
            <HeroSection />
            <BridgingGapSection />
            <AgroMetHubSection />
            <GLobalImpactSection />
            <PartnersSection />
            <AboutUsSection />
        </Flex>
    );
};

export default LandingPage;