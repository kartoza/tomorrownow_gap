import React, { createContext, useContext, useState, useEffect} from 'react';


interface GapContextInterface {
    api_swagger_url: string;
    api_docs_url: string;
}

const defaultContext: GapContextInterface = {
    'api_swagger_url': '/api/v1/docs/',
    'api_docs_url': ''
}

const GapContext = createContext(defaultContext)

interface AppProps {
    children: React.ReactNode;
};

const GapContextProvider = (props: AppProps) => {
    const [contextData, setContextData] = useState<GapContextInterface>(defaultContext)

    useEffect(() => {
        // Parse json context from gap-base-context
        setTimeout(() => {
            const element = document.getElementById('gap-base-context')
            if (element) {
                setContextData(JSON.parse(element.innerHTML))
            } else {
                console.log('Warning: no base context is found.')
            }
        }, 100)
    }, [])

    return (
        <GapContext.Provider value={contextData}>
            { props.children }
        </GapContext.Provider>
    )

}

const useGapContext = () => {
    return useContext(GapContext);
  };
  
export { GapContextProvider, useGapContext };