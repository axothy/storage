import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import ConfigPanel from './components/ConfigPanel';
import DataBrowser from './components/DataBrowser';
import Nodes from './components/Nodes';
import Header from './components/Header';

export default function App() {
    return (
        <BrowserRouter>
            <div className="flex h-screen">
                <Sidebar/>
                <div className="flex flex-col flex-1">
                    <Header/>
                    <div className="flex-1 p-6 overflow-auto">
                        <Routes>
                            <Route path="/" element={<DataBrowser/>}/>
                            <Route path="/config" element={<ConfigPanel/>}/>
                            <Route path="/nodes" element={<Nodes/>}/>
                        </Routes>
                    </div>
                </div>
            </div>
        </BrowserRouter>
    );
}
