package bigdata.utils;

public class Lang {
    
    public String lang;

    public Lang(String lang){
        this.lang = lang;
    }

    public String getConvertedLang(){

        switch(this.lang){
            // First row
            case "am" :
                return "Ahmharic";
            case "ar" :
                return "Arabic";
            case "hy" :
                return "Armenian";         
            case "eu" :
                return "Basque";                
            case "bn" :
                return "Bengali";             
            case "bs" :
                return "Bosnian";             
            case "bg" :
                return "Bulgarian";           
            case "my" :
                return "Burmese";             
            case "hr" :
                return "Croatian";               
            case "ca" :
                return "Catalan";               
            case "cs" :
                return "Czech";               
            case "da" :
                return "Danish";               
            case "nl" :
                return "Dutch";               
            case "en" :
                return "English";              
            case "et" :
                return "Estonian";               
            case "fi" :
                return "Finnish";                
            case "fr" :
                return "French";               
            case "ka" :
                return "Georgian";
                
            // Second row
            case "de" :
                return "German";      
            case "el" :
                return "Greek";
            case "gu" :
                return "Gujarati";
            case "ht" :
                return "Haitian Creole";
            case "iw" :
                return "Hebrew";
            case "hi" :
                return "Hindi";
            case "hi-Latn" :
                return "Latinized Hindi";   
            case "hu" :
                return "Hungarian";                
            case "is" :
                return "Icelandic";                
            case "in" :
                return "Indonesian";                
            case "it" :
                return "Italian";               
            case "ja" :
                return "Japanese";               
            case "kn" :
                return "Kannada";              
            case "km" :
                return "Khmer";              
            case "ko" :
                return "Korean";               
            case "lo" :
                return "Lao";               
            case "lv" :
                return "Latvian";               
            case "lt" :
                return "Lithuanian";

            // Third row
            case "ml" :
                return "Malayalam";               
            case "dv" :
                return "Maldivian";               
            case "mr" :
                return "Marathi";              
            case "ne" :
                return "Nepali";              
            case "no" :
                return "Norwegian";               
            case "or" :
                return "Oriya";               
            case "pa" :
                return "Panjabi";              
            case "ps" :
                return "Pashto";              
            case "fa" :
                return "Persian";             
            case "pl" :
                return "Polish";            
            case "pt" :
                return "Portuguese";            
            case "ro" :
                return "Romanian";              
            case "ru" :
                return "Russian";
            case "sr" :
                return "Serbian";               
            case "zh-CN" :
                return "Simplified Chinese";               
            case "sd" :
                return "Sindhi";                
            case "si" :
                return "Sinhala";
                
            // Fourth row
            case "sk" :
                return "Slovak";
            case "sl" :
                return "Slovenian";               
            case "ckb" :
                return "Sorani Kurdish";
            case "es" :
                return "Spanish";
            case "sv" :
                return "Swedish";
            case "tl" :
                return "Tagalog";
            case "ta" :
                return "Tamil";
            case "te" :
                return "Telugu";
            case "th" :
                return "Thai";
            case "bo" :
                return "Tibetan";
            case "zh-TW" :
                return "Traditional Chinese";
            case "tr":
                return "Turkish";
            case "uk" :
                return "Ukrainian";
            case "ur" :
                return "Urdu";
            case "ug" :
                return "Uyghur";
            case "vi" :
                return "Vietnamese";
            case "cy" :
                return "Welsh";
            default:
                return "unvalid country";
        }
    }
}
