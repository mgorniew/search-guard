package com.floragunn.searchguard;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Provider.Service;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

public class FipsManager {
    
    //enterprise needed?
    //do not allow sslv3,sslv2 even when configured so
    
    public static void fips() throws GeneralSecurityException {
        
        
        Security.addProvider(new BouncyCastleFipsProvider());
        
        System.out.println(
                "Java Version: " + System.getProperty("java.version") + " " + System.getProperty("java.vendor"));
        System.out.println("JVM Impl.: " + System.getProperty("java.vm.version") + " "
                + System.getProperty("java.vm.vendor") + " " + System.getProperty("java.vm.name"));
        
        String hashed = computePBKDF("myclear123upassword".toCharArray());
        System.out.println();
        
        try {
           
           final int aesMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES");
           System.out.println("Max AES key len: "+aesMaxKeyLength);
           
           final int rsaMaxKeyLength = Cipher.getMaxAllowedKeyLength("RSA");
           System.out.println("Max RSA key len: "+rsaMaxKeyLength);
           
           String md5Provider = MessageDigest.getInstance("MD5").getProvider().getClass().getName();
           System.out.println("md5Provider: "+ md5Provider);
           
           String sha1Provider = MessageDigest.getInstance("SHA-1").getProvider().getClass().getName();
           System.out.println("sha1Provider: "+ sha1Provider);
            
        } catch (final NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        
        
        Provider [] providerList = Security.getProviders();
        System.out.println("Security provider count: "+providerList.length);
        System.out.println();
        for (Provider provider : providerList)
         {
           System.out.println("Name: "  + provider.getName()+" (Version: "+provider.getVersion()+")");
           System.out.println("Class: " + provider.getClass());
           System.out.println("Information: " + provider.getInfo());

           Set<Service> serviceList = provider.getServices();
           for (Service service : serviceList)
            {
              System.out.println("    Service Type: " + service.getType() + " Algorithm " + service.getAlgorithm());
            }
         }
        
    }
    
    public static void main(String[] args) throws GeneralSecurityException {
        fips();
    }
    
    public static String PDKDF_ALGORITHM = "PBKDF2WithHmacSHA512" ;
    public static int ITERATION_COUNT = 12288 ;
    public static int SALT_LENGTH = 128 ;
    public static int DERIVED_KEY_LENGTH = 256 ;
    
    public static String computePBKDF(char[] password) throws GeneralSecurityException {
        byte[] salt = new byte[SALT_LENGTH] ;
        
        SecureRandom secRandom = new SecureRandom() ;
        secRandom.nextBytes(salt) ;

        PBEKeySpec keySpec = null ;
        try { 
            keySpec = new PBEKeySpec(password, salt, ITERATION_COUNT , DERIVED_KEY_LENGTH * 8);
        } catch(NullPointerException nullPointerExc){throw new GeneralSecurityException("Salt " + salt + "is null") ;}  
         catch(IllegalArgumentException illegalArgumentExc){throw new GeneralSecurityException("One of the argument is illegal. Salt " + salt + " is of 0 length, iteration count " + ITERATION_COUNT + " is not positive or derived key length " + DERIVED_KEY_LENGTH + " is not positive." ) ;}  

        SecretKeyFactory pbkdfKeyFactory = null ;

        try { 
            pbkdfKeyFactory = SecretKeyFactory.getInstance(PDKDF_ALGORITHM) ;
        } catch(NullPointerException nullPointExc) {throw new GeneralSecurityException("Specified algorithm " + PDKDF_ALGORITHM  + "is null") ;} 
         catch(NoSuchAlgorithmException noSuchAlgoExc) {throw new GeneralSecurityException("Specified algorithm " + PDKDF_ALGORITHM + "is not available by the provider " + pbkdfKeyFactory.getProvider().getName()) ;} 

        byte[] pbkdfHashedArray = null ; 
        try {  
            pbkdfHashedArray = pbkdfKeyFactory.generateSecret(keySpec).getEncoded() ; 
        } catch(InvalidKeySpecException invalidKeyExc) {throw new GeneralSecurityException("Specified key specification is inappropriate") ; } 
       
        return Base64.getEncoder().encodeToString(pbkdfHashedArray) ; 
}
    
    //https://gist.github.com/jtan189/3804290
    /*public static boolean validatePassword(char[] password, String goodHash)
            throws NoSuchAlgorithmException, InvalidKeySpecException
        {
            // Decode the hash into its parameters
            String[] params = goodHash.split(":");
            int iterations = Integer.parseInt(params[ITERATION_INDEX]);
            byte[] salt = fromHex(params[SALT_INDEX]);
            byte[] hash = fromHex(params[PBKDF2_INDEX]);
            // Compute the hash of the provided password, using the same salt, 
            // iteration count, and hash length
            byte[] testHash = pbkdf2(password, salt, iterations, hash.length);
            // Compare the hashes in constant time. The password is correct if
            // both hashes match.
            return slowEquals(hash, testHash);
    }*/
    
    
    public static String generateHash(char[] cs, byte[] salt, int i) {
        return null;
    }

    public static boolean checkPassword(String replaceEnvVars, char[] charArray) {
        // TODO Auto-generated method stub
        return false;
    }

    public static String sha256Hash(byte[] readAllBytes) throws NoSuchAlgorithmException {
        // TODO Auto-generated method stub
        return null;
    }

    public static byte[] hash(byte[] in, String algo) throws NoSuchAlgorithmException {
        // TODO Auto-generated method stub
        return null;
    }

    
    //Blake2bDigest
    public static byte[] fastHash(byte[] in) {
        // TODO Auto-generated method stub
        return null;
    }
    
    

}
